package backupper

import java.io.FileOutputStream
import java.security.Security
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

import akka.Done
import akka.actor.Actor
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.LoggerFactory

class EncryptedWriterActor extends Actor {
  val logger = LoggerFactory.getLogger(getClass)

//  lazy val stream = new FileOutputStream("/home/stivo/benchmark/testdir/linux-descabato-storage/blocks.kvs")
  lazy val stream = new FileOutputStream("blocks.kvs")


  Security.addProvider(new BouncyCastleProvider())
  val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")
  private val key: Array[Byte] = "0123456789012345".getBytes
  private val spec = new SecretKeySpec(key, "AES")
  cipher.init(Cipher.ENCRYPT_MODE, spec)
  override def receive: Receive = {
    case b@Block(_, _, _) =>
//      logger.info("encrypted writer done")
      var array = b.compressed.toArray
//      array = cipher.doFinal(array)
      stream.write(array)

      sender() ! true
    case Done =>
//      logger.info("Closing file")
      stream.close()
//      logger.info("Closed file")
  }
}
