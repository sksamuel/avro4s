package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class AvroOutputStreamTest extends WordSpec with Matchers with Timeouts {

  def read[T: AvroSchema2](out: ByteArrayOutputStream): GenericRecord = read(out.toByteArray)
  def read[T: AvroSchema2](bytes: Array[Byte]): GenericRecord = {
    val datum = new GenericDatumReader[GenericRecord](implicitly[AvroSchema2[T]].apply())
    val reader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datum)
    reader.hasNext
    reader.next()
  }

  "AvroOutputStream" should {
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
    "write a Some as populated union" in {
      case class Test(opt: Option[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(Some(123.456d)))
      avro.close()

      val record = read[Test](output)
      record.get("opt").toString shouldBe "123.456"
    }
    "write a None as union null" in {
      case class Test(opt: Option[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(None))
      avro.close()

      val record = read[Test](output)
      record.get("opt") shouldBe null
    }
    "write Array of primitives" in {
      case class Test(array: Array[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(Array(1d, 2d, 3d, 4d)))
      avro.close()

      val record = read[Test](output)
      record.get("array").asInstanceOf[java.util.List[Double]].asScala shouldBe Seq(1d, 2d, 3d, 4d)
    }
    "write Seq of primitives" in {
      case class Test(sequence: Seq[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(Seq(1d, 2d, 3d, 4d)))
      avro.close()

      val record = read[Test](output)
      record.get("sequence").asInstanceOf[java.util.List[Double]].asScala shouldBe Seq(1d, 2d, 3d, 4d)
    }
    "write list of primitives" in {
      case class Test(list: List[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(List(1d, 2d, 3d, 4d)))
      avro.close()

      val record = read[Test](output)
      record.get("list").asInstanceOf[java.util.List[Double]].asScala shouldBe List(1d, 2d, 3d, 4d)
    }
    "write map of strings" in {
      case class Test(map: Map[String, String])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream[Test](output)
      avro.write(Test(Map("name" -> "sammy")))
      avro.close()

      val record = read[Test](output)
      record.get("map").asInstanceOf[java.util.Map[Utf8, Utf8]].asScala.map { case (k, v) =>
        k.toString -> v.toString
      } shouldBe Map("name" -> "sammy")
    }
  }
}