package backupper.model

import akka.util.ByteString

case class FileMetadata(fd: FileDescription, blocks: ByteString)
