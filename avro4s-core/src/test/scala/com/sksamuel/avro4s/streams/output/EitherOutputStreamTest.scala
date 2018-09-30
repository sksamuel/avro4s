package com.sksamuel.avro4s.streams.output

import java.util

import com.sksamuel.avro4s.schema.Wine
import org.apache.avro.AvroRuntimeException
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

class EitherOutputStreamTest extends OutputStreamTest {

  import scala.collection.JavaConverters._

  test("write out either of primitives") {
    case class Test(z: Either[String, Int])
    writeRead(Test(Left("hello"))) { record =>
      record.get("z") shouldBe new Utf8("hello")
    }
    writeRead(Test(Right(45))) { record =>
      record.get("z") shouldBe 45
    }
  }

  test("write out either of Array") {
    case class Test(z: Either[Array[Int], String])
    writeRead(Test(Left(Array(1, 3, 4)))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[Int]].asScala shouldBe List(1, 3, 4)
    }
  }

  test("write out either of Seq") {
    case class Test(z: Either[String, Seq[String]])
    writeRead(Test(Right(Seq("c", "d")))) { record =>
      record.get("z").asInstanceOf[GenericData.Array[String]].asScala shouldBe List(new Utf8("c"), new Utf8("d"))
    }
  }

  test("write out either of enum") {
    case class Test(z: Either[Wine, Seq[String]])
    writeRead(Test(Left(Wine.Malbec))) { record =>
      record.get("z").asInstanceOf[GenericData.EnumSymbol].toString shouldBe "Malbec"
    }
  }

  test("write out either of Maps") {
    case class Test(z: Either[Array[Int], Map[String, Boolean]])
    writeRead(Test(Right(Map("a" -> true, "b" -> false)))) { record =>
      record.get("z").asInstanceOf[util.HashMap[String, Boolean]].asScala shouldBe Map(new Utf8("a") -> true, new Utf8("b") -> false)
    }
  }

  test("write out case classes") {
    case class Foo(a: String)
    case class Bar(b: Boolean)
    case class Test(z: Either[Foo, Bar])
    writeRead(Test(Left(Foo("hello")))) { record =>
      record.get("z").asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("hello")
    }
    writeRead(Test(Right(Bar(true)))) { record =>
      record.get("z").asInstanceOf[GenericRecord].get("b") shouldBe true
    }
  }

  test("throw an exception if trying to use two collection types in an either") {
    intercept[AvroRuntimeException] {
      case class Test(z: Either[Seq[String], List[Int]])
      writeRead(Test(Left(Seq("hello")))) { record =>
      }
    }
  }
}