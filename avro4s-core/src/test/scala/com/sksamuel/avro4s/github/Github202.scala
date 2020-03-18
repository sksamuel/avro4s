package com.sksamuel.avro4s.github

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroAlias, AvroInputStream, AvroOutputStream, AvroSchema}
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class Github202 extends AnyFunSuite with Matchers {

  test("Alias avro schema feature problem #202") {

    case class Version1(a: String)

    @AvroAlias("Version1")
    case class Version2(@AvroAlias("a") renamed: String, `new`: String = "default")

    val v1 = Version1("hello")
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.data[Version1].to(baos).build()
    output.write(v1)
    output.close()

    val is = AvroInputStream.data[Version2].from(baos.toByteArray).build(AvroSchema[Version1])
    val v2 = is.iterator.toList.head
    is.close()

    assert(v2.renamed == v1.a)
    assert(v2.`new` == "default")
  }



  test("Alias avro schema feature problem #202 using java with alias") {

    val writeSchema =
      """
        |{"namespace": "com.sksamuel.avro4s.github.Github202", "type": "record", "name": "ClubToRead",
        |    "fields": [
        |        {"name": "full_name", "type": "string"},
        |        {"name": "foundation_year", "type": "int"}
        |    ]
        |}
      """.stripMargin.replace("\n", "")

    val readSchema =
      """
        |{"namespace": "com.sksamuel.avro4s.github.Github202", "type": "record", "name": "ClubToRead", "aliases":["ClubToRead"],
        |"fields": [
        |        {"name": "fullName", "type": "string", "aliases": ["full_name"]},
        |        {"name": "foundationYear", "type": "int", "aliases": ["foundation_year"]}
        |    ]
        |}
      """.stripMargin.replace("\n", "")

    import java.io.ByteArrayOutputStream

    import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
    import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}

    import collection.JavaConverters._

    val writerSchema = new org.apache.avro.Schema.Parser().parse(writeSchema)
    val readerSchema = new org.apache.avro.Schema.Parser().parse(readSchema)

    val recordV1 = new GenericData.Record(writerSchema)
    recordV1.put("full_name", "cupcat")
    recordV1.put("foundation_year", 5)

    val baos = new ByteArrayOutputStream()
    val datumWriter = new GenericDatumWriter[GenericRecord](writerSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(writerSchema, baos)

    dataFileWriter.append(recordV1)

    dataFileWriter.flush()
    dataFileWriter.close()

    baos.toByteArray


    val datumReader = new GenericDatumReader[GenericRecord](readerSchema)
    val inputStream = new SeekableByteArrayInput(baos.toByteArray)
    val dataFileReader = new DataFileReader[GenericRecord](inputStream, datumReader)

    val dataFromAvro = dataFileReader.iterator().asScala.toList

    dataFileReader.close()


    dataFromAvro.head.get("fullName").toString shouldBe "cupcat"
    dataFromAvro.head.get("foundationYear").toString.toInt shouldBe 5

  }
}