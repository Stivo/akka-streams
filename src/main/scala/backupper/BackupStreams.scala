package backupper

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.stream.Collectors
import java.util.zip.GZIPOutputStream

import akka.Done
import akka.actor.{ActorSystem, Props, TypedActor, TypedProps}
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.{ByteString, Timeout}
import backupper.actors.{BlockStorageActor, BlockWriter, BlockWriterActor}
import backupper.model._
import jdk.nashorn.internal.ir.BlockStatement
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory}
import org.slf4j.LoggerFactory
import org.tukaani.xz.{FilterOptions, LZMA2Options, XZOutputStream}

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object BackupStreams {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  private val value: TypedProps[BlockStorageActor] = TypedProps.apply[BlockStorageActor](classOf[BlockStorage], classOf[BlockStorageActor])

  val blockStorageActor: BlockStorage = TypedActor(system).typedActorOf(value.withTimeout(5.minutes))
  private val value2: TypedProps[BlockWriterActor] = TypedProps.apply[BlockWriterActor](classOf[BlockWriter], classOf[BlockWriterActor])
  val blockWriter: BlockWriter = TypedActor(system).typedActorOf(value2.withTimeout(5.minutes))

  val cpuService = Executors.newFixedThreadPool(16)
  implicit val ex = ExecutionContext.fromExecutorService(cpuService)

  val standardSink = Sink.onComplete {
    case Success(x) =>
      logger.info(s"Got $x")
    case Failure(e) =>
      logger.info(s"Failure: ${e.getMessage}")
  }

  val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

  def main(args: Array[String]): Unit = {

    val service = Executors.newFixedThreadPool(5)
    implicit val ex = ExecutionContext.fromExecutorService(service)

    Await.result(blockStorageActor.startup(), 1.minute)
    val start = System.currentTimeMillis()
    val stream = Files.walk(Paths.get(args(0)))
      .collect(Collectors.toList())

    val paths = stream.asScala.filter(Files.isRegularFile(_)).filterNot(_.toAbsolutePath.toString.contains("/.git/"))
    val futures: immutable.Seq[(String, Future[Done])] = paths.map { x =>
      val string = x.toRealPath().toString
      (string, graphBased("", string))
    }.toList
    for ((filename, fut) <- futures) {
      Await.result(fut, 100.minutes)
      //      logger.info(s"Finished backing up file $filename")
    }
    blockWriter.finish()
    blockStorageActor.finish()
    system.terminate()
    service.shutdown()
    cpuService.shutdown()
    val end = System.currentTimeMillis()
    logger.info(s"Took ${end - start} ms")
  }

  implicit val timeout = Timeout(1.minute)

  def graphBased(prefix: String, filename: String): Future[Done] = {
    val logFile = Paths.get(filename)
    var fd = new FileDescription(filename, Length(logFile.toFile.length()))

    val sinkIn = Sink.ignore

    val graph = RunnableGraph.fromGraph(GraphDSL.create(sinkIn) { implicit builder =>
      sink =>
        import GraphDSL.Implicits._
        val source = FileIO.fromPath(logFile, chunkSize = 64 * 1024)

        val bcast = builder.add(Broadcast[ByteString](2))
        val merge = builder.add(Merge[Unit](2))

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

        def mapped() = Flow[Any].map(_ => ())

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

        source ~> bcast
        bcast ~> hasher ~> logger1 ~> mapped() ~> merge.in(1)

        val stream1 = bcast ~> chunker ~> createBlock ~> blockStorage
        val stream2 = stream1
        stream2 ~> sendToActor ~> sendStoredChunkToOtherActor ~> mapped() ~> merge.in(0)

        merge.out ~> sink
        ClosedShape
    })
    graph.run()
  }


}


//class CompleteFileDescription(val fileDescription: FileDescription, var blocks: Seq[BlockDescription])