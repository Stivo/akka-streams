package backupper.actors

import java.io.File

import backupper.model._
import backupper.util.Implicits._
import backupper.{BackupFileHandler, Config, JsonUser}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class BackupFileActor(val config: Config) extends BackupFileHandler with JsonUser {
  val logger = LoggerFactory.getLogger(getClass)

  private var hasChanged = false

  private var previous: Map[FileDescription, FileMetadata] = Map.empty
  private var thisBackup: Map[FileDescription, FileMetadata] = Map.empty

  private var toBeStored: Set[FileDescription] = Set.empty

  private val filename = "metadata"
  private val file = new File(config.backupDestinationFolder, filename + ".json")

  def startup(): Future[Boolean] = {
    if (file.exists()) {
      val seq = readJson[Seq[FileMetadata]](file)
      previous = seq.map(x => (x.fd, x)).toMap
    }
    Future.successful(true)
  }

  def alreadySavedFiles(): Future[Set[FileDescription]] = {
    Future.successful(previous.keySet)
  }

  override def backedUpFiles(): Future[Seq[FileMetadata]]  = {
    Future.successful(previous.values.toSeq)
  }

  override def hasAlready(fd: FileDescription): Future[Boolean] = {
    val haveAlready = previous.safeContains(fd) || toBeStored.safeContains(fd) || thisBackup.contains(fd)
    if (!haveAlready) {
      toBeStored += fd
      hasChanged = true
    }
    Future.successful(haveAlready)
  }

  override def saveFile(fileMetadata: FileMetadata): Future[Boolean] = {
    thisBackup += fileMetadata.fd -> fileMetadata
    toBeStored -= fileMetadata.fd
    Future.successful(true)
  }

  override def saveFileSameAsBefore(fd: FileDescription): Future[Boolean] = {
    thisBackup += fd -> previous(fd)
    Future.successful(true)
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      logger.info(s"Writing metadata of ${thisBackup.values.size} files")
      writeToJson(file, thisBackup.values)
      logger.info("Done Writing metadata")
    }
    Future.successful(true)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error("Actor was restarted", reason)
  }
}



