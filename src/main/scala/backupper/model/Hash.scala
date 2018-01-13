package backupper.model

import akka.util.ByteString

case class Hash(val byteString: ByteString) extends AnyVal
