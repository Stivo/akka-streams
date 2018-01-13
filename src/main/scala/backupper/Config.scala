package backupper

import java.io.File
import java.security.MessageDigest

import akka.util.ByteString
import backupper.util._

class Config(val backupDestinationFolder: File) {


  var compressionMode: CompressionMode = CompressionMode.none

  var hashMethod: HashMethod = HashMethod.sha256

  def hashLength: Int = hashMethod.getDigestLength

  def newHasher: MessageDigest = hashMethod.newInstance()

  var key: ByteString = _

  def newWriter(file: File): FileWriter = {
    if (key == null) {
      new SimpleFileWriter(file)
    } else {
      new EncryptedFileWriter(file, key)
    }
  }

  def newReader(file: File): FileReader = {
    if (key == null) {
      new SimpleFileReader(file)
    } else {
      new EncryptedFileReader(file, key)
    }
  }

}
