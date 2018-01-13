package backupper

import java.security.MessageDigest

import backupper.util.{CompressionMode, HashMethod}

class Config {

  var compressionMode: CompressionMode = CompressionMode.none

  var hashMethod: HashMethod = HashMethod.sha256
  def hashLength: Int = hashMethod.getDigestLength
  def newHasher: MessageDigest = hashMethod.newInstance()

}
