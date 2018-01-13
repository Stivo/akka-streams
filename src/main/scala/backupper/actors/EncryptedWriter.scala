package backupper.actors

import java.io.FileOutputStream
import java.security.Security
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

import akka.Done
import backupper.model.{Block, Length, StoredChunk}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class BlockWriterActor extends BlockWriter {
  val logger = LoggerFactory.getLogger(getClass)

  private val currentFileName = "blocks.kvs"
  //  lazy val stream = new FileOutputStream("/home/stivo/benchmark/testdir/linux-descabato-storage/blocks.kvs")
  private lazy val stream = new FileOutputStream(currentFileName, true)

  private var pos = 0L

  Security.addProvider(new BouncyCastleProvider())
  val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")
  private val key: Array[Byte] = "0123456789012345".getBytes
  private val spec = new SecretKeySpec(key, "AES")
  cipher.init(Cipher.ENCRYPT_MODE, spec)

  override def saveBlock(block: Block): Future[StoredChunk] = {
    var posBefore = pos
    stream.write(block.compressed.toArray)
    pos += block.compressed.length
    Future.successful(StoredChunk(currentFileName, block.hash, posBefore, Length(block.compressed.length)))
  }

  override def finish(): Future[Done] = {
    stream.close()
    Future.successful(Done)
  }
}


trait BlockWriter {
  def saveBlock(block: Block): Future[StoredChunk]

  def finish(): Future[Done]
}