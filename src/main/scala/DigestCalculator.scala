import java.security.MessageDigest

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import akka.util.ByteString

class DigestCalculator(algorithm: String) extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("DigestCalculator.in")
  val out = Outlet[ByteString]("DigestCalculator.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val digest = MessageDigest.getInstance(algorithm)

    def tryPull(): Unit = {
      if (!hasBeenPulled(in)) {
        pull(in)
      }
    }

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        tryPull()
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val chunk = grab(in)
        digest.update(chunk.toArray)
        tryPull()
      }

      override def onUpstreamFinish(): Unit = {
        emit(out, ByteString(digest.digest()))
        completeStage()
      }
    })

  }
}
