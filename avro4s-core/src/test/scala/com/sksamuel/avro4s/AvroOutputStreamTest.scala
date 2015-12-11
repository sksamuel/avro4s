package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

class AvroOutputStreamTest extends WordSpec with Matchers with Timeouts {

  val michelangelo = Artist("michelangelo", 1475, 1564, "Caprese", Seq("sculpture", "fresco"))
  val raphael = Artist("raphael", 1483, 1520, "florence", Seq("painter", "architect"))

  import AvroImplicits._

  def read[T: AvroSchema2](out: ByteArrayOutputStream): GenericRecord = read(out.toByteArray)
  def read[T: AvroSchema2](bytes: Array[Byte]): GenericRecord = {
    val datum = new GenericDatumReader[GenericRecord](implicitly[AvroSchema2[T]].apply())
    val reader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datum)
    reader.hasNext
    reader.next()
  }

  "AvroSerializer" should {
    "write out strings" in {

      case class Test(str: String)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test("sammy"))
      avro.close()

      val record = read[Test](output)
      record.get("str").toString shouldBe "sammy"
    }
    "write out booleans" in {

      case class Test(bool: Boolean)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(true))
      avro.close()

      val record = read[Test](output)
      record.get("bool").toString shouldBe "true"
    }
    "write out longs" in {

      case class Test(l: Long)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(56l))
      avro.close()

      val record = read[Test](output)
      record.get("l").toString shouldBe "56"
    }
    "write out ints" in {

      case class Test(i: Int)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(666))
      avro.close()

      val record = read[Test](output)
      record.get("i").toString shouldBe "666"
    }
    "write out doubles" in {

      case class Test(d: Double)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(123.456))
      avro.close()

      val record = read[Test](output)
      record.get("d").toString shouldBe "123.456"
    }
    "write out eithers of primitives for lefts" in {

      case class Test(e: Either[String, Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(Left("sam")))
      avro.close()

      val record = read[Test](output)
      record.get("e").toString shouldBe "sam"
    }
    "write out eithers of primitives for rights" in {

      case class Test(e: Either[String, Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(Right(45.4d)))
      avro.close()

      val record = read[Test](output)
      record.get("e").toString shouldBe "45.4"
    }
    //    "write Some as populated union" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val option = OptionWriteExample(Option("sammy"))
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[OptionWriteExample](path)
    //      output.write(option)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[OptionWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("option").toString shouldBe "sammy"
    //    }
    //    "write None as union null" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val option = OptionWriteExample(None)
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[OptionWriteExample](path)
    //      output.write(option)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[OptionWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("option") shouldBe null
    //    }
    //    "supporting writing Eithers" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val either = EitherWriteExample(Left("sammy"), Right(123l))
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[EitherWriteExample](path)
    //      output.write(either)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[EitherWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("either1").toString shouldBe "sammy"
    //      rec.get("either2").toString.toLong shouldBe 123l
    //    }
    //    "supporting writing Bytes" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val bytes = ByteWriteExample(Array[Byte](1,2,3))
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[ByteWriteExample](path)
    //      output.write(bytes)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[ByteWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("bytes").asInstanceOf[ByteBuffer].array shouldBe Array[Byte](1,2,3)
    //    }
    //    "supporting writing Longs" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val bytes = LongWriteExample(3453453l)
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[LongWriteExample](path)
    //      output.write(bytes)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[LongWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("long").toString.toLong shouldBe 3453453l
    //    }
    //    "supporting writing Doubles" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val bytes = DoubleWriteExample(123412.522)
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[DoubleWriteExample](path)
    //      output.write(bytes)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[DoubleWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("double").toString.toDouble shouldBe 123412.522
    //    }
    //    "supporting writing Booleans" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val example = BooleanWriteExample(true, false)
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[BooleanWriteExample](path)
    //      output.write(example)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[BooleanWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("left").toString.toBoolean shouldBe true
    //      rec.get("right").toString.toBoolean shouldBe false
    //    }
    //    "support writing Arrays of primitives" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val example = ArrayWriteExample(Array("elton", "john"), Array(true, false, true))
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[ArrayWriteExample](path)
    //      output.write(example)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[ArrayWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("strings").asInstanceOf[org.apache.avro.generic.GenericData.Array[Utf8]].asScala.map(_.toString).toSet shouldBe Set("elton", "john")
    //      rec.get("booleans").asInstanceOf[org.apache.avro.generic.GenericData.Array[Boolean]].asScala.toSet shouldBe Set(true, false, true)
    //    }
    //    "support writing Seqs of primitives" in {
    //      val path = Files.createTempFile("AvroSerializerTest", ".avro")
    //      path.toFile.deleteOnExit()
    //
    //      val example = SeqWriteExample(Seq("elton", "john"), Seq(true, false, true))
    //
    //      import AvroImplicits._
    //      val output = AvroOutputStream[SeqWriteExample](path)
    //      output.write(example)
    //      output.close()
    //
    //      val datum = new GenericDatumReader[GenericRecord](schemaFor[SeqWriteExample].schema)
    //      val reader = new DataFileReader[GenericRecord](path.toFile, datum)
    //
    //      reader.hasNext
    //      val rec = reader.next()
    //      rec.get("strings").asInstanceOf[org.apache.avro.generic.GenericData.Array[Utf8]].asScala.map(_.toString).toSet shouldBe Set("elton", "john")
    //      rec.get("booleans").asInstanceOf[org.apache.avro.generic.GenericData.Array[Boolean]].asScala.toSet shouldBe Set(true, false, true)
    //    }
  }
}

case class OptionWriteExample(option: Option[String])

case class EitherWriteExample(either1: Either[String, Boolean], either2: Either[String, Long])

case class ByteWriteExample(bytes: Array[Byte])

case class DoubleWriteExample(double: Double)

case class LongWriteExample(long: Long)

case class BooleanWriteExample(left: Boolean, right: Boolean)

case class ArrayWriteExample(strings: Array[String], booleans: Array[Boolean])

case class SeqWriteExample(strings: Seq[String], booleans: Seq[Boolean])