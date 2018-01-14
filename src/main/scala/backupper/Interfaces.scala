package backupper

import java.io.File

import akka.actor.TypedActor
import akka.util.ByteString
import backupper.model._
import backupper.util.Implicits._
import backupper.util.Json
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import scala.concurrent.Future

trait BlockStorage extends LifeCycle {
  def read(hash: Hash): Future[ByteString]

  def hasAlready(block: Block): Future[Boolean]

  def save(storedChunk: StoredChunk): Future[Boolean]


}

trait BackupFileHandler extends LifeCycle with TypedActor.PreRestart {
  def backedUpFiles(): Future[Seq[FileMetadata]]

  def hasAlready(fileDescription: FileDescription): Future[Boolean]

  def saveFile(fileMetadata: FileMetadata): Future[Boolean]

  def saveFileSameAsBefore(fd: FileDescription): Future[Boolean]
}

trait JsonUser {
  def config: Config

  def readJson[T: Manifest](file: File): T = {
    val reader = config.newReader(file)
    val bs = reader.readAllContent()
    Json.mapper.readValue[T](bs.asInputStream)
  }

  def writeToJson[T](file: File, value: T) = {
    val writer = config.newWriter(file)
    val bytes = Json.mapper.writer(new DefaultPrettyPrinter()).writeValueAsBytes(value)
    writer.write(ByteString(bytes))
    writer.finish()
  }
}

trait LifeCycle {
  def startup(): Future[Boolean]
  def finish(): Future[Boolean]
}