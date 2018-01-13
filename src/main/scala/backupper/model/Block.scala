package backupper.model

import akka.util.ByteString
import backupper.CustomByteArrayOutputStream
import net.jpountz.lz4.LZ4Factory
import org.slf4j.LoggerFactory

case class Block(blockId: BlockId, content: ByteString, hash: Hash) {
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

object Block {
  val factory = LZ4Factory.fastestJavaInstance()
}