package backupper

import java.io.File
import java.util.concurrent.CompletableFuture

import akka.actor.TypedActor
import akka.util.ByteString
import backupper.model._
import backupper.util.{CompressedStream, CompressionMode, Json}
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait BlockStorage extends LifeCycle {
  def read(hash: Hash): Future[ByteString]

  def hasAlready(block: Block): Future[Boolean]
  def hasAlreadyJava(block: Block): CompletableFuture[Block] = {
    val fut = new CompletableFuture[Block]()
    hasAlready(block).map { bool =>
      block.isAlreadySaved = bool
      fut.complete(block)
    }
    fut
  }

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
    val decompressed = CompressedStream.decompress(bs)
    Json.mapper.readValue[T](decompressed)
  }

  def writeToJson[T](file: File, value: T) = {
    val writer = config.newWriter(file)
    val bytes = Json.mapper.writer(new DefaultPrettyPrinter()).writeValueAsBytes(value)
    val compressed = CompressedStream.compress(ByteString(bytes), CompressionMode.deflate)
    writer.write(compressed)
    writer.finish()
  }
}

trait LifeCycle {
  def startup(): Future[Boolean]
  def finish(): Future[Boolean]
}