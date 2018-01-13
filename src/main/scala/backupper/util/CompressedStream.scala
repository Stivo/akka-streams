package backupper.util

import java.io.{InputStream, OutputStream}
import java.util.zip._

import akka.util.ByteString
import backupper.CustomByteArrayOutputStream
import backupper.util.Implicits._
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import org.apache.commons.io.IOUtils
import org.iq80.snappy.{SnappyFramedInputStream, SnappyFramedOutputStream}
import org.tukaani.xz.{LZMA2Options, XZInputStream, XZOutputStream}

/**
  * Adds compressors and decompressors
  */
object CompressedStream {

  def compress(content: ByteString, compressor: CompressionMode): ByteString = {
    val byte = compressor.getByte()
    val baos = new CustomByteArrayOutputStream(content.length + 80)
    baos.write(byte)
    val wrapped = getCompressor(byte, baos, Some(content.length))
    wrapped.write(content)
    wrapped.close()
    baos.toByteString
  }

  def roundUp(x: Int): Int = {
    var z = 4096
    while (z < x) {
      z = z * 2
    }
    z
  }

  def getCompressor(byte: Int, out: OutputStream, size: Option[Int] = None): OutputStream = {
    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPOutputStream(out, 65536)
      case CompressionMode.lzma =>
        val opt = new LZMA2Options()
        opt.setDictSize(1024 * 1024)
        size.foreach { s => opt.setDictSize(Math.max(1024 * 4, s + 10)) }
        new XZOutputStream(out, opt)
      case CompressionMode.snappy => new SnappyFramedOutputStream(out)
      case CompressionMode.deflate => new DeflaterOutputStream(out)
      case CompressionMode.lz4 => new LZ4BlockOutputStream(out, 1 << 12, LZ4Factory.fastestJavaInstance().fastCompressor())
      case CompressionMode.lz4hc => new LZ4BlockOutputStream(out, 1 << 12, LZ4Factory.fastestJavaInstance().highCompressor())
      case CompressionMode.none => out
      case CompressionMode.smart => throw new IllegalArgumentException("Choose a concrete implementation")
    }
  }

  def decompress(input: ByteString): InputStream = {
    val in = input.asInputStream()
    val byte = in.read()

    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPInputStream(in, 65536)
      case CompressionMode.lzma => new XZInputStream(in)
      case CompressionMode.snappy => new SnappyFramedInputStream(in, false)
      case CompressionMode.deflate => new InflaterInputStream(in)
      case CompressionMode.lz4 => new LZ4BlockInputStream(in)
      case CompressionMode.lz4hc => new LZ4BlockInputStream(in)
      case _ => in
    }
  }

  def decompressToBytes(input: ByteString): ByteString = {
    val stream = decompress(input)
    val baos = new CustomByteArrayOutputStream()
    IOUtils.copy(stream, baos, 100 * 1024)
    IOUtils.closeQuietly(stream)
    IOUtils.closeQuietly(baos)
    baos.toByteString()
  }

}