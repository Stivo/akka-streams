package backupper

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.stream.Collectors

import akka.Done
import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.{ByteString, Timeout}
import backupper.actors.{BackupFileActor, BlockStorageActor, BlockWriter, BlockWriterActor}
import backupper.model._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable
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

  val cpuService = Executors.newFixedThreadPool(16)
  implicit val ex = ExecutionContext.fromExecutorService(cpuService)

  def main(args: Array[String]): Unit = {

    val service = Executors.newFixedThreadPool(5)
    implicit val ex = ExecutionContext.fromExecutorService(service)

    Await.result(blockStorageActor.startup(), 1.minute)
    val start = System.currentTimeMillis()
    val stream = Files.walk(Paths.get(args(0)))
      .collect(Collectors.toList())

    val actors: Seq[LifeCycle] = Seq(backupFileActor, blockStorageActor, blockWriter)
    for (fut <- actors.map(_.startup())) {
      Await.result(fut, 1.minute)
    }

    val paths = stream.asScala.filter(Files.isRegularFile(_)).filterNot(_.toAbsolutePath.toString.contains("/.git/"))
    val futures: immutable.Seq[(String, Future[Done])] = paths.map { x =>
      val string = x.toRealPath().toString
      (string, graphBased("", string))
    }.toList
    for ((filename, fut) <- futures) {
      Await.result(fut, 100.minutes)
    }
    for (fut <- actors.map(_.finish())) {
      Await.result(fut, 1.minute)
    }
    system.terminate()
    service.shutdown()
    cpuService.shutdown()
    val end = System.currentTimeMillis()
    logger.info(s"Took ${end - start} ms")
  }

  implicit val timeout = Timeout(1.minute)

  def graphBased(prefix: String, filename: String): Future[Done] = {
    val logFile = Paths.get(filename)
    val fd = new FileDescription(logFile.toFile)

    val eventualBoolean = backupFileActor.hasAlready(fd)
    val hasAlready = Await.result(eventualBoolean, 1.minute)
    if (hasAlready) {
      backupFileActor.saveFileSameAsBefore(fd)
      Future.successful(Done)
    } else {
      val sinkIn = Sink.ignore

      val graph = RunnableGraph.fromGraph(GraphDSL.create(sinkIn) { implicit builder =>
        sink =>
          import GraphDSL.Implicits._
          val source = FileIO.fromPath(logFile, chunkSize = 64 * 1024)

          val fileContent = builder.add(Broadcast[ByteString](2))
          val hashedBlocks = builder.add(Broadcast[Block](2))
          val zip = builder.add(Zip[ByteString, Hash]())
          val waitForCompletion = builder.add(Merge[Unit](2))

          val hasher = new DigestCalculator("MD5")
          val chunker = new Framer()
          val logger1 = new Logger("log1", true)

          val createBlock = Flow[ByteString].zipWithIndex.map { case (x, i) =>
            val hash = Hash(ByteString(MessageDigest.getInstance("MD5").digest(x.toArray)))
            val b = Block(BlockId(fd, i.toInt), x, hash)
            b
          }

          val blockStorage = Flow[Block].mapAsync(10) { x =>
            blockStorageActor.hasAlready(x).map { fut =>
              (x, fut)
            }
          }.filter(!_._2).map(_._1).mapAsync(10)(x => Future(x.compress))

          def mapToUnit() = Flow[Any].map(_ => ())

          def streamCounter[T](name: String) = Flow[T].zipWithIndex.map { case (x, i) =>
            logger.info(s"$prefix $name: Element $i")
            x
          }

          val sendToActor = Flow[Block].mapAsync(2) { b =>
            blockWriter.saveBlock(b)
          }

          val sendStoredChunkToOtherActor = Flow[StoredChunk].mapAsync(5) { b =>
            blockStorageActor.save(b)
          }

          val concatHashes = Flow[Block].map(_.hash.byteString).fold(ByteString.empty)(_ ++ _)

          val createFileDescription = Flow[(ByteString, Hash)].mapAsync(1) { case (hashlist, completeHash) =>
            fd.hash = completeHash
            val out = FileMetadata(fd, hashlist)
            logger.info(out.toString)
            backupFileActor.saveFile(out)
          }

          source ~> fileContent
          fileContent ~> hasher ~> zip.in1
          fileContent ~> chunker ~> createBlock ~> hashedBlocks

          hashedBlocks ~> blockStorage ~> sendToActor ~> sendStoredChunkToOtherActor ~> mapToUnit() ~> waitForCompletion.in(0)
          hashedBlocks ~> concatHashes ~> zip.in0

          zip.out ~> createFileDescription ~> mapToUnit() ~> waitForCompletion.in(1)

          waitForCompletion.out ~> sink
          ClosedShape
      })
      graph.run()
    }
  }


}


//class CompleteFileDescription(val fileDescription: FileDescription, var blocks: Seq[BlockDescription])