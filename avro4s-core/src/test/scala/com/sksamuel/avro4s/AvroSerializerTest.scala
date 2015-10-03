package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.nio.file.Files

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroSerializerTest extends WordSpec with Matchers with Timeouts {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence", Seq("painter", "architect"))

  import AvroImplicits._

  import scala.collection.JavaConverters._

  "AvroSerializer" should {
    "write out simple records" in {

      val painters = Seq(michelangelo, raphael)
      val path = Files.createTempFile("AvroSerializerTest", ".avro")
      path.toFile.deleteOnExit()

      val s = schemaFor[Artist]

      val writer = AvroOutputStream[Artist](path)
      writer.write(painters)
      writer.close()

      val datumReader = new GenericDatumReader[GenericRecord](s.schema)
      val dataFileReader = new DataFileReader[GenericRecord](path.toFile, datumReader)

      dataFileReader.hasNext
      val rec1 = dataFileReader.next(new GenericData.Record(s.schema))
      rec1.get("name").toString shouldBe michelangelo.name
      rec1.get("yearOfBirth").toString.toInt shouldBe michelangelo.yearOfBirth
      rec1.get("yearOfDeath").toString.toInt shouldBe michelangelo.yearOfDeath
      val styles1 = rec1.get("styles").asInstanceOf[GenericData.Array[Utf8]].asScala.toSet
      styles1.map(_.toString) shouldBe michelangelo.styles.toSet

      dataFileReader.hasNext
      val rec2 = dataFileReader.next(new GenericData.Record(s.schema))
      rec2.get("name").toString shouldBe raphael.name
      rec2.get("yearOfBirth").toString.toInt shouldBe raphael.yearOfBirth
      rec2.get("yearOfDeath").toString.toInt shouldBe raphael.yearOfDeath
      val styles2 = rec2.get("styles").asInstanceOf[GenericData.Array[Utf8]].asScala.toSet
      styles2.map(_.toString) shouldBe raphael.styles.toSet
    }
    "write Some as populated union" in {
      val path = Files.createTempFile("AvroSerializerTest", ".avro")
      path.toFile.deleteOnExit()

      val option = OptionWriteExample(Option("sammy"))

      import AvroImplicits._
      val output = AvroOutputStream[OptionWriteExample](path)
      output.write(option)
      output.close()

      val datum = new GenericDatumReader[GenericRecord](schemaFor[OptionWriteExample].schema)
      val reader = new DataFileReader[GenericRecord](path.toFile, datum)

      reader.hasNext
      val rec = reader.next()
      rec.get("option").toString shouldBe "sammy"
    }
    "write None as union null" in {
      val path = Files.createTempFile("AvroSerializerTest", ".avro")
      path.toFile.deleteOnExit()

      val option = OptionWriteExample(None)

      import AvroImplicits._
      val output = AvroOutputStream[OptionWriteExample](path)
      output.write(option)
      output.close()

      val datum = new GenericDatumReader[GenericRecord](schemaFor[OptionWriteExample].schema)
      val reader = new DataFileReader[GenericRecord](path.toFile, datum)

      reader.hasNext
      val rec = reader.next()
      rec.get("option") shouldBe null
    }
    "supporting writing Eithers" in {
      val path = Files.createTempFile("AvroSerializerTest", ".avro")
      path.toFile.deleteOnExit()

      val either = EitherWriteExample(Left("sammy"), Right(123l))

      import AvroImplicits._
      val output = AvroOutputStream[EitherWriteExample](path)
      output.write(either)
      output.close()

      val datum = new GenericDatumReader[GenericRecord](schemaFor[EitherWriteExample].schema)
      val reader = new DataFileReader[GenericRecord](path.toFile, datum)

      reader.hasNext
      val rec = reader.next()
      rec.get("either1").toString shouldBe "sammy"
      rec.get("either2").toString.toLong shouldBe 123l
    }
    "supporting writing Bytes" in {
      val path = Files.createTempFile("AvroSerializerTest", ".avro")
      path.toFile.deleteOnExit()

      val bytes = ByteWriteExample(Array[Byte](1,2,3))

      import AvroImplicits._
      val output = AvroOutputStream[ByteWriteExample](path)
      output.write(bytes)
      output.close()

      val datum = new GenericDatumReader[GenericRecord](schemaFor[ByteWriteExample].schema)
      val reader = new DataFileReader[GenericRecord](path.toFile, datum)

      reader.hasNext
      val rec = reader.next()
      rec.get("bytes").asInstanceOf[ByteBuffer].array shouldBe Array[Byte](1,2,3)
    }
  }
}

case class OptionWriteExample(option: Option[String])

case class EitherWriteExample(either1: Either[String, Boolean], either2: Either[String, Long])

case class ByteWriteExample(bytes: Array[Byte])