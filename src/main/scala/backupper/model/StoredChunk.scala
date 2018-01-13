package backupper.model

case class StoredChunk(val file: String, val hash: Hash, val startPos: Long, val length: Length)
