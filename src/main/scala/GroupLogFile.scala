import java.nio.file.{Path, Paths}
import java.security.MessageDigest
import java.util

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, IOResult}
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.collection.script.Message
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object GroupLogFile {

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  val standardSink = Sink.onComplete {
    case Success(x) =>
      println(s"Got $x")
    case Failure(e) =>
      println(s"Failure: ${e.getMessage}")
  }

  val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer

    // execution context

//    graphBased("1: ", "D:\\upload\\Civilization - Baba Yetu.mp4")

    chunkFile("1: ", "D:\\upload\\Civilization - Baba Yetu.mp4")
//    chunkFile("2: ", "D:\\upload\\Civilization - Baba Yetu.mp4.ident")
//    chunkFile("3: ", "D:\\upload\\Civilization - Baba Yetu.mp4.shifted")
    //    system.terminate()
  }

  def graphBased(prefix: String, filename: String): Unit = {
    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
//      val zip = builder.add(Zip[Message, Trigger]())
//      elements ~> zip.in0
//      triggerSource ~> zip.in1
//      zip.out ~> Flow[(Message, Trigger)].map { case (msg, trigger) => msg } ~> sink
      ClosedShape
    })
  }


  private def chunkFile(prefix: String, file: String): Any = {
    // read lines from a log file
    val logFile = Paths.get(file)

    //    exampleParsing(logFile)
    val flow1 = FileIO.fromPath(logFile)
      //              .via(new Logger())
      .via(new Framer(prefix))
      //      .via(new Logger())
//      .to(Sink)
//    val flow1 = Source.fromPublisher(flow1Pub)
    val flow2 = flow1
      .mapAsync(10)(bs =>
        Future {
          MessageDigest.getInstance("MD5").digest(bs.toArray[Byte])
        }
      )
      .map { x =>
        println(s"Got ${x.length} with content ${util.Arrays.toString(x)}")
        x
      }
      //      .via(new DigestCalculator("MD5"))
      //      .via(new Logger(prefix, printContent = true))
    val digest = MessageDigest.getInstance("MD5")
    val flow3 = flow1
      .via(new DigestCalculator("MD5"))
      .via(new Logger("totalHash", true))

//      .runWith(standardSink)
  }

  private def exampleParsing(logFile: Path) = {
    FileIO.fromPath(logFile).
      // parse chunks of bytes into lines
      via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true)).
      map(_.utf8String).
      map {
        case line@LoglevelPattern(level) => (level, line)
        case line@other => ("OTHER", line)
      }.
      // group them by log level
      groupBy(5, _._1).
      fold(("", List.empty[String])) {
        case ((_, list), (level, line)) => (level, line :: list)
      }.
      // write lines of each group to a separate file
      mapAsync(parallelism = 5) {
      case (level, groupList) =>
        Source(groupList.reverse).map(line => ByteString(line + "\n")).runWith(FileIO.toPath(Paths.get(s"target/log-$level.txt")))
    }.
      mergeSubstreams.
      runWith(Sink.onComplete {
        case Success(_) =>
          system.terminate()
        case Failure(e) =>
          println(s"Failure: ${e.getMessage}")
          system.terminate()
      })
  }
}