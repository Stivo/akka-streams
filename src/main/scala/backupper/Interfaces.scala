package backupper

import backupper.model.{Block, FileDescription, StoredChunk}

import scala.concurrent.Future

trait BlockStorage extends LifeCycle {
  def hasAlready(block: Block): Future[Boolean]

  def save(storedChunk: StoredChunk): Future[Boolean]

}

trait BackupFileHandler {
  def saveFile(fileDescription: FileDescription)
}

trait LifeCycle {
  def startup(): Future[Boolean]
  def finish(): Future[Boolean]
}