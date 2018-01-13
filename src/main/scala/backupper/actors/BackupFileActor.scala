package backupper.actors

import java.io.{File, FileOutputStream, OutputStream}
import java.util.zip.GZIPOutputStream

import backupper.BackupFileHandler
import backupper.model._
import backupper.util.Implicits._
import backupper.util.Json
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class BackupFileActor extends BackupFileHandler {
  val logger = LoggerFactory.getLogger(getClass)

  private var hasChanged = false

  private var previous: Map[FileDescription, FileMetadata] = Map.empty
  private var thisBackup: Map[FileDescription, FileMetadata] = Map.empty

  private var toBeStored: Set[FileDescription] = Set.empty

  private val filename = "metadata"
  private val file = new File(filename + ".json")

  def startup(): Future[Boolean] = {
    if (file.exists()) {
      previous = Json.mapper.readValue[Seq[FileMetadata]](file).map(x => (x.fd, x)).toMap
    }
    Future.successful(true)
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
      writeAsJson()
      writeAsJsonGz()
      writeSmile()
    }
    Future.successful(true)
  }

  private def writeAsJson() = {
    val stream: OutputStream = new FileOutputStream(filename + ".json")
    Json.mapper.writer(new DefaultPrettyPrinter()).writeValue(stream, thisBackup.values)
    stream.close()
  }

  private def writeAsJsonGz() = {
    var stream: OutputStream = new FileOutputStream(filename + ".json.gz")
    stream = new GZIPOutputStream(stream)
    Json.mapper.writer(new DefaultPrettyPrinter()).writeValue(stream, thisBackup.values)
    stream.close()
  }

  private def writeSmile() = {
    var stream: OutputStream = new FileOutputStream(filename + ".smile")
    stream = new GZIPOutputStream(stream)
    Json.smileMapper.writer().writeValue(stream, thisBackup.values)
    stream.close()
  }

}



