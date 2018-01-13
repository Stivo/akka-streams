package backupper

import akka.util.ByteString
import backupper.model._

import scala.concurrent.Future

trait BlockStorage extends LifeCycle {
  def read(hash: Hash): Future[ByteString]

  def hasAlready(block: Block): Future[Boolean]

  def save(storedChunk: StoredChunk): Future[Boolean]

}

trait BackupFileHandler extends LifeCycle {
  def backedUpFiles(): Future[Seq[FileMetadata]]

  def hasAlready(fileDescription: FileDescription): Future[Boolean]

  def saveFile(fileMetadata: FileMetadata): Future[Boolean]

  def saveFileSameAsBefore(fd: FileDescription): Future[Boolean]
}

trait LifeCycle {
  def startup(): Future[Boolean]
  def finish(): Future[Boolean]
}