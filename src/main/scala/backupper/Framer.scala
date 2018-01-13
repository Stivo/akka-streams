package backupper

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

class Framer(prefix: String = "") extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("Framer.in")
  val out = Outlet[ByteString]("Framer.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var byteString = ByteString.empty
    private val buzHash = new BuzHash(64)

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (!hasBeenPulled(in)) {
          pull(in)
        }
      }
    })

    setHandler(in, new InHandler {

      override def onPush(): Unit = {
        val chunk = grab(in)
        val array = chunk.toArray
        var pos = 0
        val end = array.length
        while (pos < end) {
          val i = buzHash.updateAndReportBoundary(array, pos, array.length - pos, 20)
          if (i > 0) {
            val bytes = Array.ofDim[Byte](i)
            System.arraycopy(array, pos, bytes, 0, i)
            byteString = byteString ++ ByteString(bytes)
            pos += i
            //println(s"${prefix} Emitting ${byteString.size}")
            emit(out, byteString)
            byteString = ByteString.empty
            buzHash.reset()
          } else {
            val bytes = Array.ofDim[Byte](end - pos)
            System.arraycopy(array, pos, bytes, 0, end - pos)
            byteString = byteString ++ ByteString(bytes)
            pos = end
          }
        }
        if (!hasBeenPulled(in)) {
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        emit(out, byteString)
        completeStage()
      }
    })

  }
}

//val digest: Source[ByteString, NotUsed] = data.via(new DigestCalculator("SHA-256"))