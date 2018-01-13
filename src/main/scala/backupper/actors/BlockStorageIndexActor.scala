package backupper.actors

import java.io.File

import akka.util.ByteString
import backupper.model.{Block, Hash, StoredChunk}
import backupper.util.Implicits._
import backupper.{BlockStorage, Config, JsonUser}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BlockStorageIndexActor(val config: Config, val chunkHandler: ChunkHandler) extends BlockStorage with JsonUser {

  val logger = LoggerFactory.getLogger(getClass)

  private var hasChanged = false

  private var map: Map[Hash, StoredChunk] = Map.empty

  private var toBeStored: Set[Hash] = Set.empty

  private val value = "blockIndex"
  val file = new File(config.backupDestinationFolder, value + ".json")

  def startup(): Future[Boolean] = {
    Future {
      if (file.exists()) {
        val seq = readJson[Seq[StoredChunk]](file)
        map = seq.map(x => (x.hash, x)).toMap
      }
      true
    }
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
    chunkHandler.read(chunk)
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      logger.info("Started writing blocks metadata")
      writeToJson(file, map.values)
      logger.info("Done Writing blocks metadata")
    }
    Future.successful(true)
  }

}



