package backupper.model

import akka.util.ByteString
import backupper.Config
import backupper.util.CompressedStream
import net.jpountz.lz4.LZ4Factory
import org.slf4j.LoggerFactory

case class Block(blockId: BlockId, content: ByteString, hash: Hash) {
  val logger = LoggerFactory.getLogger(getClass)

  var compressed: ByteString = _
  var encrypted: ByteString = _

  def compress(config: Config): Block = {
    compressed = CompressedStream.compress(content, config.compressionMode)
    this
  }

}
