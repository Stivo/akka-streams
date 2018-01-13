package backupper

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.stream.Collectors
import java.util.zip.GZIPOutputStream

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.{ByteString, Timeout}
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

  val blockStorageActor = system.actorOf(Props(classOf[BlockStorageActor]))
  val encryptedWriter = system.actorOf(Props(classOf[EncryptedWriterActor]))
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

    val service = Executors.newFixedThreadPool(50)
    implicit val ex = ExecutionContext.fromExecutorService(service)

    val start = System.currentTimeMillis()
    val stream = Files.walk(Paths.get(args(0)))
      .collect(Collectors.toList())

    val paths = stream.asScala.par.filter(Files.isRegularFile(_)).filterNot(_.toAbsolutePath.toString.contains("/.git/")).seq
    val futures: immutable.Seq[(String, Future[Done])] = paths.map { x =>
      val string = x.toRealPath().toString
      (string, graphBased("", string))
    }.toList
    for ((filename, fut) <- futures) {
      Await.result(fut, 100.minutes)
      //      logger.info(s"Finished backing up file $filename")
    }
    encryptedWriter ! Done
    system.terminate()
    service.shutdown()
    cpuService.shutdown()
    val end = System.currentTimeMillis()
    logger.info(s"Took ${end - start} ms")
  }

  implicit val timeout = Timeout(1 minute)

  def graphBased(prefix: String, filename: String): Future[Done] = {
    val logFile = Paths.get(filename)
    var fd = new FileDescription(filename, logFile.toFile.length())

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
          val hash = ByteString(MessageDigest.getInstance("MD5").digest(x.toArray))
          val b = Block(BlockId(fd, i.toInt), x, hash)
          b
        }

        val blockStorage = Flow[Block].mapAsync(10) { x =>
          (blockStorageActor ? x).asInstanceOf[Future[(Block, Boolean)]]
        }.filter(_._2).map(_._1).mapAsync(10)(x => Future(x.compress))

        def mapped() = Flow[Any].map(_ => ())

        def streamCounter[T](name: String) = Flow[T].zipWithIndex.map { case (x, i) =>
          logger.info(s"$prefix $name: Element $i")
          x
        }

        val sendToActor = Flow[Block].mapAsync(2) { b =>
          (encryptedWriter ? b).asInstanceOf[Future[Boolean]]
        }

        source ~> bcast
        bcast ~> hasher ~> logger1 ~> mapped() ~> merge.in(1)

        val stream1 = bcast ~> chunker ~> createBlock ~> blockStorage
        val stream2 = stream1
        stream2 ~> sendToActor ~> mapped() ~> merge.in(0)

        merge.out ~> sink
        ClosedShape
    })
    graph.run()
  }


}

case class FileDescription(var path: String, var size: Long) {
  var hash: ByteString = _
}

case class BlockId(fd: FileDescription, blockNr: Int)

object Block {
  val factory = LZ4Factory.fastestJavaInstance()
}

case class Block(blockId: BlockId, content: ByteString, hash: ByteString) {
  val logger = LoggerFactory.getLogger(getClass)

  var compressed: ByteString = _
  var encrypted: ByteString = _

  def compress: Block = {
    val stream = new CustomByteArrayOutputStream(content.length + 10)
//    val options = new LZMA2Options()
//    val dictSize = Math.max(4 * 1024, content.length + 20)
//    options.setDictSize(dictSize)
//        val comp = new GZIPOutputStream(stream)
//    val comp = new XZOutputStream(stream, options)
//    comp.write(content.toArray)
//    comp.close()
//    this.compressed = ByteString(stream.toByteArray)
    val compressed = Block.factory.fastCompressor().compress(content.toArray)
    this.compressed = ByteString(compressed)
    //    logger.info(s"Compressed $hash")
    this
  }

}