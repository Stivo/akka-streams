package backupper.util

import java.io.{File, RandomAccessFile}

import akka.util.ByteString

trait FileReader {

  def file: File

  def readAllContent(): ByteString

  def readChunk(position: Long, length: Long): ByteString

  def close(): Unit

}

class SimpleFileReader(val file: File) extends FileReader {
  private val randomAccessReader = new RandomAccessFile(file, "r")
  var position = 0

  override def readAllContent(): ByteString = {
    readChunk(0, file.length())
  }

  override def readChunk(position: Long, length: Long): ByteString = {
    randomAccessReader.seek(position)
    val value = Array.ofDim[Byte](length.toInt)
    randomAccessReader.readFully(value)
    ByteString(value)
  }

  override def close(): Unit = {
    randomAccessReader.close()
  }
}