import java.nio.file.Paths
import java.security.MessageDigest
import java.util.zip.GZIPOutputStream

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.{ByteString, Timeout}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

object BackupStreams {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  val blockStorageActor = system.actorOf(Props(classOf[BlockStorageActor]))
  val encryptedWriter = system.actorOf(Props(classOf[EncryptedWriterActor]))

  val standardSink = Sink.onComplete {
    case Success(x) =>
      logger.info(s"Got $x")
    case Failure(e) =>
      logger.info(s"Failure: ${e.getMessage}")
  }

  val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer

    // execution context

    //    graphBased("1: ", "D:\\upload\\Civilization - Baba Yetu.mp4")

    val fut1 = graphBased("1: ", "D:\\upload\\Civilization - Baba Yetu.mp4")
//    val fut2 = graphBased("2: ", "D:\\upload\\Civilization - Baba Yetu.mp4.ident")
//    val fut3 = graphBased("3: ", "D:\\upload\\Civilization - Baba Yetu.mp4.shifted")
    Await.result(fut1, 1.minute)
//    Await.result(fut2)
//    Await.result(fut3)
    encryptedWriter ! Done
    system.terminate()
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
        }.filter(_._2).map(_._1).mapAsync(10)(x => Future (x.compress))

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

case class Block(blockId: BlockId, content: ByteString, hash: ByteString) {
  val logger = LoggerFactory.getLogger(getClass)

  var compressed: ByteString = _
  var encrypted: ByteString = _

  def compress: Block = {
    val stream = new CustomByteArrayOutputStream()
    val gzip = new GZIPOutputStream(stream)
    gzip.write(content.toArray)
    gzip.close()
    this.compressed = ByteString(stream.toByteArray)
    logger.info(s"Compressed $hash")
    this
  }

}