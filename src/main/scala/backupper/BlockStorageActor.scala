package backupper

import akka.actor.Actor
import akka.util.ByteString
import org.slf4j.LoggerFactory

class BlockStorageActor extends Actor {
  val logger = LoggerFactory.getLogger(getClass)

  var map: Map[ByteString, Long] = Map.empty

  var i = 0L

  override def receive: Receive = {
    case b@Block(_, _, hash) =>
      if (!map.contains(hash)) {
//        logger.info(s"${hashCode()} has not yet seen $hash")
        map += hash -> i
        i += 1
        sender() ! Tuple2(b, true)
      } else {
//        logger.info(s"${hashCode()} has already seen $hash")
        sender() ! Tuple2(b, false)
      }
  }
}
