package com.sksamuel.avro4s

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.concurrent.Timeouts
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

sealed trait Dibble
case class Dobble(str: String) extends Dibble
case class Dabble(dbl: Double) extends Dibble

case class Drapper(dibble: Dibble)

case class Test1(wine: Wine)
case class Test2(dec: BigDecimal)

case class Foo(str: String, boolean: Boolean)

case class NestedListFoo(foos: List[Foo])
case class NestedListDouble(doubles: List[Double])
case class NestedListBools(booleans: List[Boolean])

case class NestedSetDoubles(set: Set[Double])
case class NestedSetTest(set: Set[Foo])

case class NestedSeqDoubles(sequence: Seq[Double])
case class NestedSeqTest(seq: Seq[Foo])

case class NestedMapTest(map: Map[String, Foo])

case class ValueWrapper(valueClass: ValueClass)
case class ValueClass(value: String) extends AnyVal

case class EitherCaseClasses(e: Either[Test1, Test2])

class AvroOutputStreamTest extends WordSpec with Matchers with Timeouts {

  def read[T](out: ByteArrayOutputStream)(implicit schema: SchemaFor[T]): GenericRecord = read(out.toByteArray)
  def read[T](bytes: Array[Byte])(implicit schema: SchemaFor[T]): GenericRecord = {
    val datumReader = new GenericDatumReader[GenericRecord](schema())
    val dataFileReader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(bytes), datumReader)
     new Iterator[GenericRecord] {
      override def hasNext: Boolean = dataFileReader.hasNext
      override def next(): GenericRecord = dataFileReader.next
    }.toList.head
  }

  "AvroOutputStream" should {
    "support java enums" in {

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test1](output)
      avro.write(Test1(Wine.Malbec))
      avro.close()

      val record = read[Test1](output)
      record.get("wine").toString shouldBe Wine.Malbec.name
    }
    "write big decimal" in {

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test2](output)
      avro.write(Test2(123.456789))
      avro.close()

      val record = read[Test2](output)
      new String(record.get("dec").asInstanceOf[ByteBuffer].array) shouldBe "123.456789"
    }
    "write out strings" in {
      case class Test(str: String)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test("sammy"))
      avro.close()

      val record = read[Test](output)
      record.get("str").toString shouldBe "sammy"
    }
    "write out booleans" in {
      case class Test(bool: Boolean)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(true))
      avro.close()

      val record = read[Test](output)
      record.get("bool").toString shouldBe "true"
    }
    "write out longs" in {
      case class Test(l: Long)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(56l))
      avro.close()

      val record = read[Test](output)
      record.get("l").toString shouldBe "56"
    }
    "write out ints" in {
      case class Test(i: Int)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(666))
      avro.close()

      val record = read[Test](output)
      record.get("i").toString shouldBe "666"
    }
    "write out doubles" in {
      case class Test(d: Double)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(123.456))
      avro.close()

      val record = read[Test](output)
      record.get("d").toString shouldBe "123.456"
    }
    "write out eithers of primitives for lefts" in {
      case class Test(e: Either[String, Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(Left("sam")))
      avro.close()

      val record = read[Test](output)
      record.get("e").toString shouldBe "sam"
    }
    "write out eithers of primitives for rights" in {
      case class Test(e: Either[String, Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(Right(45.4d)))
      avro.close()

      val record = read[Test](output)
      record.get("e").toString shouldBe "45.4"
    }
    "write eithers of case classes" in {
      val output1 = new ByteArrayOutputStream
      val avro1 = AvroOutputStream.data[EitherCaseClasses](output1)
      avro1.write(EitherCaseClasses(Left(Test1(Wine.CabSav))))
      avro1.close()

      val record1 = read[EitherCaseClasses](output1)
      record1.get("e").toString shouldBe """{"wine": "CabSav"}"""

      val output2 = new ByteArrayOutputStream
      val avro2 = AvroOutputStream.data[EitherCaseClasses](output2)
      avro2.write(EitherCaseClasses(Right(Test2(14.56))))
      avro2.close()

      val record2 = read[EitherCaseClasses](output2)
      record2.get("e").toString shouldBe """{"dec": {"bytes": "14.56"}}"""
    }
    "write a Some as populated union" in {
      case class Test(opt: Option[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(Some(123.456d)))
      avro.close()

      val record = read[Test](output)
      record.get("opt").toString shouldBe "123.456"
    }
    "write a None as union null" in {
      case class Test(opt: Option[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(None))
      avro.close()

      val record = read[Test](output)
      record.get("opt") shouldBe null
    }
    "write Array of doubles" in {
      case class Test(array: Array[Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(Array(1d, 2d, 3d, 4d)))
      avro.close()

      val record = read[Test](output)
      record.get("array").asInstanceOf[java.util.List[Double]].asScala shouldBe Seq(1d, 2d, 3d, 4d)
    }
    "write Seq of doubles" in {

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedSeqDoubles](output)
      avro.write(NestedSeqDoubles(Seq(1d, 2d, 3d, 4d)))
      avro.close()

      val record = read[NestedSeqDoubles](output)
      record.get("sequence").asInstanceOf[java.util.List[Double]].asScala shouldBe Seq(1d, 2d, 3d, 4d)
    }
    "write deep nested maps" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Level1](output)
      avro.write(Level1(Level2(Level3(Level4(Map("a" -> "b"))))))
      avro.close()

      val record = read[Level1](output)
      record.toString shouldBe """{"level2": {"level3": {"level4": {"str": {"a": "b"}}}}}"""
    }
    "write Seq of nested classes" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedSeqTest](output)
      avro.write(NestedSeqTest(List(Foo("sam", true), Foo("ham", false))))
      avro.close()

      val record = read[NestedSeqTest](output)
      val data = record.get("seq").asInstanceOf[java.util.List[GenericRecord]].asScala.toList
      data.head.get("str").toString shouldBe "sam"
      data.last.get("str").toString shouldBe "ham"
    }
    "write Set of doubles" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedSetDoubles](output)
      avro.write(NestedSetDoubles(Set(1d, 9d, 9d, 9d, 9d)))
      avro.close()

      val record = read[NestedSetDoubles](output)
      record.get("set").asInstanceOf[java.util.List[Double]].asScala.toSet shouldBe Set(1d, 9d)
    }
    "write Set of nested classes" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedSetTest](output)
      avro.write(NestedSetTest(Set(Foo("sam", true), Foo("ham", false))))
      avro.close()

      val record = read[NestedSetTest](output)
      val actual = record.get("set").asInstanceOf[java.util.List[GenericRecord]].asScala.toSet
      actual.map(_.get("str").toString) shouldBe Set("sam", "ham")
      actual.map(_.get("boolean").toString.toBoolean) shouldBe Set(true, false)
    }
    "write list of doubles" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedListDouble](output)
      avro.write(NestedListDouble(List(1d, 2d, 3d, 4d)))
      avro.close()

      val record = read[NestedListDouble](output)
      record.get("doubles").asInstanceOf[java.util.List[Double]].asScala shouldBe List(1d, 2d, 3d, 4d)
    }
    "write list of booleans" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedListBools](output)
      avro.write(NestedListBools(List(true, false, true)))
      avro.close()

      val record = read[NestedListBools](output)
      record.get("booleans").asInstanceOf[java.util.List[Double]].asScala shouldBe List(true, false, true)
    }
    "write list of nested classes" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedListFoo](output)
      avro.write(NestedListFoo(List(Foo("sam", false))))
      avro.close()

      val record = read[NestedListFoo](output)
      record.get("foos").toString shouldBe """[{"str": "sam", "boolean": false}]"""
    }
    "write map of strings" in {
      case class Test(map: Map[String, String])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(Map(("name", "sammy"))))
      avro.close()

      val record = read[Test](output)
      record.get("map").asInstanceOf[java.util.Map[Utf8, Utf8]].asScala.map { case (k, v) =>
        (k.toString, v.toString)
      } shouldBe Map(("name", "sammy"))
    }
    "write map of doubles" in {
      case class Test(map: Map[String, Double])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(Map(("name", 12.3d))))
      avro.close()

      val record = read[Test](output)
      record.get("map").asInstanceOf[java.util.Map[Utf8, java.lang.Double]].asScala.map { case (k, v) =>
        (k.toString, v.toString.toDouble)
      } shouldBe Map(("name", 12.3d))
    }
    "write map of booleans" in {
      case class Test(map: Map[String, Boolean])

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Test](output)
      avro.write(Test(Map(("name", true))))
      avro.close()

      val record = read[Test](output)
      record.get("map").asInstanceOf[java.util.Map[Utf8, java.lang.Boolean]].asScala.map { case (k, v) =>
        (k.toString, v.toString.toBoolean)
      } shouldBe Map(("name", true))
    }
    "write map of nested classes" in {
      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[NestedMapTest](output)
      avro.write(NestedMapTest(Map(("foo", Foo("sam", false)))))
      avro.close()

      val record = read[NestedMapTest](output)
      val map = record.get("map").asInstanceOf[java.util.Map[Utf8, GenericRecord]].asScala.map { case (k, v) =>
        (k.toString, v)
      }
      map("foo").get("str").toString shouldBe "sam"
      map("foo").get("boolean").toString shouldBe "false"
    }
    "support extends AnyVal" in {
      val instance = ValueWrapper(ValueClass("bob"))

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[ValueWrapper](output)
      avro.write(instance)
      avro.close()

      val record = read[ValueWrapper](output)
      record.get("valueClass").asInstanceOf[GenericRecord].get("value").toString shouldBe "bob"
    }
    "support scala enums" in {
      val instance = ScalaEnums(Colours.Amber)

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[ScalaEnums](output)
      avro.write(instance)
      avro.close()

      val record = read[ScalaEnums](output)
      record.get("value").toString shouldBe "Amber"
    }
    "support UUIDs" in {
      val instance = Ids(UUID.randomUUID())

      val output = new ByteArrayOutputStream
      val avro = AvroOutputStream.data[Ids](output)
      avro.write(instance)
      avro.close()

      val record = read[Ids](output)
      UUID.fromString(record.get("myid").toString) shouldBe instance.myid
    }
  }
}

object Colours extends Enumeration {
  val Red, Amber, Green = Value
}
case class ScalaEnums(value: Colours.Value)


