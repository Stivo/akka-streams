package backupper.actors

import java.io.File

import akka.util.ByteString
import backupper.model.{Block, Length, StoredChunk}
import backupper.{Config, JsonUser, LifeCycle}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class ChunkStorageActor(val config: Config) extends ChunkHandler {
  val logger = LoggerFactory.getLogger(getClass)

  private val currentFileName = "blocks.kvs"
  val destinationFile = new File(config.backupDestinationFolder, currentFileName)

  private lazy val writer = config.newWriter(destinationFile)
  private lazy val reader = config.newReader(destinationFile)

  override def saveBlock(block: Block): Future[StoredChunk] = {
    val posBefore = writer.write(block.compressed)
    Future.successful(StoredChunk(currentFileName, block.hash, posBefore, Length(block.compressed.length)))
  }

  override def finish(): Future[Boolean] = {
    writer.finish()
    reader.close()
    Future.successful(true)
  }

  override def startup(): Future[Boolean] = {
    // nothing to do
    Future.successful(true)
  }

  override def read(storedChunk: StoredChunk): Future[ByteString] = {
    Future.successful(reader.readChunk(storedChunk.startPos, storedChunk.length.size))
  }
}


trait ChunkHandler extends LifeCycle {
  def saveBlock(block: Block): Future[StoredChunk]
  def read(storedChunk: StoredChunk): Future[ByteString]
}