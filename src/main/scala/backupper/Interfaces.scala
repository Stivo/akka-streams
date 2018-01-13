package backupper

import backupper.model.{Block, FileDescription, FileMetadata, StoredChunk}

import scala.concurrent.Future

trait BlockStorage extends LifeCycle {
  def hasAlready(block: Block): Future[Boolean]

  def save(storedChunk: StoredChunk): Future[Boolean]

}

trait BackupFileHandler extends LifeCycle {
  def hasAlready(fileDescription: FileDescription): Future[Boolean]

  def saveFile(fileMetadata: FileMetadata): Future[Boolean]

  def saveFileSameAsBefore(fd: FileDescription): Future[Boolean]
}

trait LifeCycle {
  def startup(): Future[Boolean]
  def finish(): Future[Boolean]
}