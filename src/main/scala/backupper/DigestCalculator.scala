package backupper

import java.security.MessageDigest

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import backupper.model.Hash
import backupper.util.HashMethod

class DigestCalculator(hashMethod: HashMethod) extends GraphStage[FlowShape[ByteString, Hash]] {
  val in = Inlet[ByteString]("DigestCalculator.in")
  val out = Outlet[Hash]("DigestCalculator.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val digest = hashMethod.newInstance()

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
        emit(out, Hash(ByteString(digest.digest())))
        completeStage()
      }
    })

  }
}
