package backupper.util

import akka.util.ByteString
import backupper.model.{Hash, Length, StoredChunk}
import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator, JsonParser, Version}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JavaType, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Json {

  //  def write[T](t: T, stream: OutputStream) = {
  //    mapper.writer().writeValue(stream, t)
  //  }
  //
  //  def read[T](content: InputStream) = {
  //    mapper.reader().readValue[T](content)
  //  }

  val mapper = new CustomObjectMapper()
  val smileMapper = new CustomObjectMapper(smile = true)

  def main(args: Array[String]): Unit = {
    tryMap
  }

  private def tryMap = {
    var map: Map[Hash, StoredChunk] = Map.empty
    val hash = Hash(ByteString.apply("Hello"))
    map += hash -> StoredChunk("Hello", hash, 0, Length(500))
    roundTrip(map.toSeq)
  }

  private def tryHash = {
    val hash = (5, Hash(ByteString.apply("Hello")))
    roundTrip(hash)
  }

  private def roundTrip[T: Manifest](map: T) = {
    println(map)
    val json = mapper.writeValueAsString(map)
    println(json)
    val clazz = manifest[T].runtimeClass
    println(s"Class is $clazz")
    val deserialized = mapper.readValue[T](json)
    println(deserialized)
  }
}

class CustomObjectMapper(smile: Boolean = false) extends ObjectMapper(if (smile) new SmileFactory() else new JsonFactory()) with ScalaObjectMapper {

  override def constructType[T](implicit m: Manifest[T]): JavaType = {
    val ByteStringName = classOf[ByteString].getName
    m.runtimeClass.getName match {
      case ByteStringName => constructType(classOf[ByteString])
      case _ => super.constructType[T]
    }
  }

  registerModule(DefaultScalaModule)


  class BaWrapperDeserializer extends StdDeserializer[ByteString](classOf[ByteString]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext): ByteString = {
      val bytes = jp.readValueAs(classOf[Array[Byte]])
      ByteString(bytes)
    }
  }

  class BaWrapperSerializer extends StdSerializer[ByteString](classOf[ByteString]) {
    def serialize(ba: ByteString, jg: JsonGenerator, prov: SerializerProvider): Unit = {
      jg.writeBinary(ba.toArray)
    }
  }

  class HashWrapperDeserializer extends StdDeserializer[Hash](classOf[Hash]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext): Hash = {
      val bytes = jp.readValueAs(classOf[Array[Byte]])
      Hash(ByteString(bytes))
    }
  }

  class HashWrapperSerializer extends StdSerializer[Hash](classOf[Hash]) {
    def serialize(ba: Hash, jg: JsonGenerator, prov: SerializerProvider): Unit = {
      jg.writeBinary(ba.byteString.toArray)
    }
  }

  val testModule = new SimpleModule("DeScaBaTo", new Version(0, 4, 0, null, "ch.descabato", "core"))
  testModule.addDeserializer(classOf[ByteString], new BaWrapperDeserializer())
  testModule.addSerializer(classOf[ByteString], new BaWrapperSerializer())
  testModule.addDeserializer(classOf[Hash], new HashWrapperDeserializer())
  testModule.addSerializer(classOf[Hash], new HashWrapperSerializer())
  registerModule(testModule)
}