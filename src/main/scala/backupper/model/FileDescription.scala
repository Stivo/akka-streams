package backupper.model

import akka.util.ByteString
import backupper.CustomByteArrayOutputStream
import net.jpountz.lz4.LZ4Factory
import org.slf4j.LoggerFactory

case class FileDescription(val path: String, val size: Length) {
  var hash: Hash = _
}

