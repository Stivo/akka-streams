package backupper.actors

import java.io.{File, FileOutputStream, OutputStream}
import java.util.zip.GZIPOutputStream

import backupper.BlockStorage
import backupper.model.{Block, Hash, StoredChunk}
import backupper.util.Implicits._
import backupper.util.Json
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import org.slf4j.LoggerFactory
import org.tukaani.xz.LZMAOutputStream

import scala.concurrent.Future

class BlockStorageActor extends BlockStorage {
  val logger = LoggerFactory.getLogger(getClass)

  private var hasChanged = false

  private var map: Map[Hash, StoredChunk] = Map.empty

  private var toBeStored: Set[Hash] = Set.empty

  private val value = "blockIndex"
  val file = new File(value + ".json")

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

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      writeAsJson()
      writeAsJsonGz()
      writeSmile()
    }
    Future.successful(true)
  }

  private def writeAsJson() = {
    var stream: OutputStream = new FileOutputStream(file)
    //    stream = new GZIPOutputStream(stream)
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
    stream = new GZIPOutputStream(stream)
    Json.smileMapper.writer().writeValue(stream, map.values)
    stream.close()
  }
}



