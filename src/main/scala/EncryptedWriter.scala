import java.io.FileOutputStream

import akka.Done
import akka.actor.Actor
import org.slf4j.LoggerFactory

class EncryptedWriterActor extends Actor {
  val logger = LoggerFactory.getLogger(getClass)

  lazy val stream = new FileOutputStream("blocks.kvs")

  override def receive: Receive = {
    case b@Block(_, _, _) =>
      logger.info("encrypted writer done")
      stream.write(b.compressed.toArray)
      stream.flush()
      sender() ! true
    case Done =>
      stream.close()
  }
}
