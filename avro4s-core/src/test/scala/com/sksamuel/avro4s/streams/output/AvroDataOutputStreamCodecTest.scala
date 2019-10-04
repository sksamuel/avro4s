package com.sksamuel.avro4s.streams.output

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema, Encoder}
import org.apache.avro.file.{CodecFactory, DataFileStream}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord, GenericRecordBuilder}
import org.scalatest.{Matchers, WordSpec}

class AvroDataOutputStreamCodecTest extends WordSpec with Matchers {

  case class Composer(name: String, birthplace: String, compositions: Seq[String])
  val schema = AvroSchema[Composer]
  val ennio = Composer("ennio morricone", "rome", Seq("legend of 1900", "ecstasy of gold"))

  "AvroDataOutputStream" should {
    "include schema" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.data[Composer].to(baos).build(schema)
      output.write(ennio)
      output.close()
      new String(baos.toByteArray) should include("birthplace")
      new String(baos.toByteArray) should include("compositions")
    }

    "include zstandard coded in metadata when serialized with zstandard" ignore {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.data[Composer].to(baos).withCodec(CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL)).build(schema)
      output.write(ennio)
      output.close()
      new String(baos.toByteArray) should include("zstandard")
    }

    "include deflate coded in metadata when serialized with deflate" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.data[Composer].to(baos).withCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL)).build(schema)
      output.write(ennio)
      output.close()
      new String(baos.toByteArray) should include("deflate")
    }

    "include bzip2 coded in metadata when serialized with bzip2" in {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.data[Composer].to(baos).withCodec(CodecFactory.bzip2Codec).build(schema)
      output.write(ennio)
      output.close()
      new String(baos.toByteArray) should include("bzip2")
    }

    "serialize generic record" in {
      import scala.collection.JavaConverters._
      val record = new GenericRecordBuilder(schema)
        .set("name", "ennio morricone")
        .set("birthplace", "rome")
        .set("compositions", List("legend of 1900", "ecstasy of gold").asJava)
        .build()

      implicit val encoder: Encoder[GenericRecord] = (r, _, _) => r

      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.data[GenericRecord].to(baos).build(schema)
      output.write(record)
      output.close()

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val reader = new DataFileStream[GenericRecord](bais, new GenericDatumReader[GenericRecord](schema))
      val result = reader.iterator().asScala.toList
      result should have size 1
      result.head shouldBe record
    }
  }
}
