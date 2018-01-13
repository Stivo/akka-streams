package backupper

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.{ExecutorService, Executors}
import java.util.stream.Collectors

import akka.Done
import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import akka.util.{ByteString, Timeout}
import backupper.actors.{BackupFileActor, BlockStorageActor, BlockWriter, BlockWriterActor}
import backupper.model._
import backupper.util.{CompressedStream, CompressionMode}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object BackupStreams {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  private val blockStorageProps: TypedProps[BlockStorageActor] = TypedProps.apply[BlockStorageActor](classOf[BlockStorage], classOf[BlockStorageActor])
  val blockStorageActor: BlockStorage = TypedActor(system).typedActorOf(blockStorageProps.withTimeout(5.minutes))

  private val backupFileActorProps: TypedProps[BackupFileActor] = TypedProps.apply[BackupFileActor](classOf[BackupFileHandler], classOf[BackupFileActor])
  val backupFileActor: BackupFileHandler = TypedActor(system).typedActorOf(backupFileActorProps.withTimeout(5.minutes))

  private val blockWriterProps: TypedProps[BlockWriterActor] = TypedProps.apply[BlockWriterActor](classOf[BlockWriter], classOf[BlockWriterActor])
  val blockWriter: BlockWriter = TypedActor(system).typedActorOf(blockWriterProps.withTimeout(5.minutes))

  val cpuService = Executors.newFixedThreadPool(10)
  implicit val ex = ExecutionContext.fromExecutorService(cpuService)

  val config = new Config()
  config.compressionMode = CompressionMode.lzma

  def main(args: Array[String]): Unit = {
//    FileUtils.deleteDirectory(new File("backup"))
    FileUtils.deleteDirectory(new File("restored"))
    new File("backup").mkdir()
    val service = Executors.newFixedThreadPool(5)
    implicit val ex = ExecutionContext.fromExecutorService(service)

    val actors: Seq[LifeCycle] = Seq(backupFileActor, blockStorageActor, blockWriter)
    for (fut <- actors.map(_.startup())) {
      Await.result(fut, 1.minute)
    }

    val start = System.currentTimeMillis()
//    backup(args, service)
    restore(args, service)
    val end = System.currentTimeMillis()
    logger.info(s"Took ${end - start} ms")

    for (fut <- actors.map(_.finish())) {
      Await.result(fut, 1.minute)
    }
    system.terminate()
    service.shutdown()
    cpuService.shutdown()
  }

  private def backup(args: Array[String], service: ExecutorService): Unit = {
    val stream = Files.walk(Paths.get(args(0)))
      .collect(Collectors.toList())

    val paths = stream.asScala.filter(Files.isRegularFile(_)).filterNot(_.toAbsolutePath.toString.contains("/.git/"))
    val function: Future[Done] = Source.fromIterator[Path](() => paths.iterator).mapAsync(10) { path =>
      backup(path)
    }.runWith(Sink.ignore)
    Await.result(function, 10.hours)
  }

  implicit val timeout = Timeout(1.minute)

  def backup(path: Path): Future[Done] = {
    val fd = new FileDescription(path.toFile)

    backupFileActor.hasAlready(fd).flatMap { hasAlready =>
      if (hasAlready) {
        backupFileActor.saveFileSameAsBefore(fd)
        Future.successful(Done)
      } else {
        val sinkIn = Sink.ignore

        val graph = RunnableGraph.fromGraph(GraphDSL.create(sinkIn) { implicit builder =>
          sink =>
            import GraphDSL.Implicits._
            val source = FileIO.fromPath(path, chunkSize = 64 * 1024)

            val fileContent = builder.add(Broadcast[ByteString](2))
            val hashedBlocks = builder.add(Broadcast[Block](2))
            val toFileDescription = builder.add(Zip[ByteString, Hash]())
            val waitForCompletion = builder.add(Merge[Unit](2))

            val hasher = new DigestCalculator(config.hashMethod)
            val chunker = new Framer()

            val createBlock = Flow[ByteString].zipWithIndex.map { case (x, i) =>
              val hash = Hash(ByteString(config.newHasher.digest(x.toArray)))
              val b = Block(BlockId(fd, i.toInt), x, hash)
              b
            }

            def newBuffer[T](bufferSize: Int = 100) = Flow[T].buffer(bufferSize, OverflowStrategy.backpressure)

            val blockStorage = Flow[Block].mapAsync(5) { x =>
              blockStorageActor.hasAlready(x).map { fut =>
                (x, fut)
              }
            }.filter(!_._2).map(_._1).mapAsync(8)(x => Future(x.compress(config)))

            def mapToUnit() = Flow[Any].map(_ => ())

            def streamCounter[T](name: String) = Flow[T].zipWithIndex.map { case (x, i) =>
              logger.info(s"$path $name: Element $i")
              x
            }

            val sendToWriter = Flow[Block].mapAsync(2) { b =>
              blockWriter.saveBlock(b)
            }

            val sendToBlockIndex = Flow[StoredChunk].mapAsync(2) { b =>
              blockStorageActor.save(b)
            }

            val concatHashes = Flow[Block].map(_.hash.byteString).fold(ByteString.empty)(_ ++ _)

            val createFileDescription = Flow[(ByteString, Hash)].mapAsync(1) { case (hashlist, completeHash) =>
              fd.hash = completeHash
              val out = FileMetadata(fd, hashlist)
              backupFileActor.saveFile(out)
            }

            source ~> newBuffer[ByteString](100) ~> fileContent
            fileContent ~> hasher ~> toFileDescription.in1
            fileContent ~> chunker ~> createBlock ~> newBuffer[Block](20) ~> hashedBlocks

            hashedBlocks ~> blockStorage ~> newBuffer[Block](20) ~> sendToWriter ~> sendToBlockIndex ~> mapToUnit() ~> waitForCompletion.in(0)
            hashedBlocks ~> concatHashes ~> toFileDescription.in0

            toFileDescription.out ~> createFileDescription ~> mapToUnit() ~> waitForCompletion.in(1)

            waitForCompletion.out ~> sink
            ClosedShape
        })
        graph.run()
      }
    }
  }

  def restore(args: Array[String], service: ExecutorService) = {
    val restoreDest = new File("restored")
    restoreDest.mkdirs()
    var finished: Boolean = false
    val eventualMetadatas = backupFileActor.backedUpFiles()
    eventualMetadatas.foreach { files =>
      for (file <- files) {
        val stream = new FileOutputStream(new File(restoreDest, file.fd.path.split("\\\\").last))
        val source = Source.fromIterator[ByteString](() => file.blocks.grouped(config.hashLength))
        Await.result(source.mapAsync(10) { hashBytes =>
          val hash = Hash(hashBytes)
          blockStorageActor.read(hash)
        }.mapAsync(10) { bs =>
          Future(CompressedStream.decompressToBytes(bs))
        }.runForeach { x =>
          stream.write(x.toArray)
        }, 1.minute)
        stream.close()
      }
      finished = true
    }
    while (!finished) {
      Thread.sleep(500)
    }
  }

}

