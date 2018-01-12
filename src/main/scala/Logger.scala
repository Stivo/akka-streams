import javax.xml.bind.DatatypeConverter

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

class Logger(prefix: String, printContent: Boolean = false) extends GraphStage[FlowShape[ByteString, ByteString]] {

  val in = Inlet[ByteString]("Logger.in")
  val out = Outlet[ByteString]("Logger.out")

  override val shape: FlowShape[ByteString, ByteString] = FlowShape.of(in, out)

  def base64(x: ByteString): String = DatatypeConverter.printBase64Binary(x.toArray)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var readyFrames: Seq[ByteString] = Seq.empty

    var counter = 0

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
//        if (readyFrames.isEmpty) {
//          pull(in)
//        } else {
//          emit(out, readyFrames.head)
//          readyFrames = readyFrames.tail
//        }
        pull(in)
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val chunk: ByteString = grab(in)
        val content = if (printContent) {
          s" with content ${base64(chunk)}"
        } else {
          ""
        }

        println(s"${prefix} ${counter} Got chunk of length ${chunk.size}$content")
        counter += 1
        push(out, chunk)
        readyFrames = readyFrames :+ chunk
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }
    })

  }


}
