package backupper.actors

import java.io.{File, FileOutputStream, OutputStream, RandomAccessFile}
import java.util.zip.GZIPOutputStream

import akka.util.ByteString
import backupper.BlockStorage
import backupper.model.{Block, Hash, StoredChunk}
import backupper.util.Implicits._
import backupper.util.Json
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class BlockStorageActor extends BlockStorage {
  val logger = LoggerFactory.getLogger(getClass)

  private var hasChanged = false

  private var map: Map[Hash, StoredChunk] = Map.empty

  private var toBeStored: Set[Hash] = Set.empty

  private val value = "backup/blockIndex"
  val file = new File(value + ".json")
  lazy val raf = new RandomAccessFile("backup/blocks.kvs", "r")

  def startup(): Future[Boolean] = {
    if (file.exists()) {
      map = Json.mapper.readValue[Seq[StoredChunk]](file).map(x => (x.hash, x)).toMap
    }
    Future.successful(true)
  }

  override def hasAlready(block: Block): Future[Boolean] = {
    val haveAlready = map.safeContains(block.hash) || toBeStored.safeContains(block.hash)
    if (!haveAlready) {
      toBeStored += block.hash
      hasChanged = true
    }
    Future.successful(haveAlready)
  }

  override def save(storedChunk: StoredChunk): Future[Boolean] = {
    map += storedChunk.hash -> storedChunk
    toBeStored -= storedChunk.hash
    Future.successful(true)
  }

  def read(hash: Hash): Future[ByteString] = {
    val chunk: StoredChunk = map(hash)
    raf.seek(chunk.startPos)
    val value = Array.ofDim[Byte](chunk.length.size.toInt)
    raf.readFully(value)
    Future.successful(ByteString(value))
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      logger.info("Started writing blocks metadata")
      writeAsJson()
      writeAsJsonGz()
      writeSmile()
      logger.info("Done Writing blocks metadata")
    }
    Future.successful(true)
  }

  private def writeAsJson() = {
    var stream: OutputStream = new FileOutputStream(file)
    Json.mapper.writer(new DefaultPrettyPrinter()).writeValue(stream, map.values)
    stream.close()
  }

  private def writeAsJsonGz() = {
    var stream: OutputStream = new FileOutputStream(value + ".json.gz")
    stream = new GZIPOutputStream(stream)
    Json.mapper.writer(new DefaultPrettyPrinter()).writeValue(stream, map.values)
    stream.close()
  }

  private def writeSmile() = {
    var stream: OutputStream = new FileOutputStream(value + ".smile")
    Json.smileMapper.writer().writeValue(stream, map.values)
    stream.close()
  }

}



