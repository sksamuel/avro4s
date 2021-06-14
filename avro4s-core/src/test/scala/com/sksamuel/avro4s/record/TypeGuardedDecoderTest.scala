package com.sksamuel.avro4s.record

import com.sksamuel.avro4s.{AvroSchema, Decoder, TypeGuardedDecoding}
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.ByteBuffer
import java.util.UUID
import scala.jdk.CollectionConverters._

class TypeGuardedDecoderTest extends AnyWordSpec with Matchers {
  "TypeGuardedDecoding" should {

    "support strings" in {

      val schema = AvroSchema[String]
      val typeGuard = TypeGuardedDecoding[String].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe true
      typeGuard.isDefinedAt(Utf8("hello")) shouldBe true
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
    }

    "support booleans" in {

      val schema = AvroSchema[Boolean]
      val typeGuard = TypeGuardedDecoding[Boolean].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(Utf8("hello")) shouldBe false
      typeGuard.isDefinedAt(false) shouldBe true
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
    }

    "support ints" in {

      val schema = AvroSchema[Int]
      val typeGuard = TypeGuardedDecoding[Int].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe true
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
    }

    "support longs" in {

      val schema = AvroSchema[Long]
      val typeGuard = TypeGuardedDecoding[Long].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123L) shouldBe true
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
    }

    "support doubles" in {

      val schema = AvroSchema[Double]
      val typeGuard = TypeGuardedDecoding[Double].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123L) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe true
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
    }

    "support maps" in {

      val schema = AvroSchema[Map[String, String]]
      val typeGuard = TypeGuardedDecoding[Map[String, String]].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe true
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
    }

    "support seqs" in {

      val schema = AvroSchema[Seq[String]]
      val typeGuard = TypeGuardedDecoding[Seq[String]].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(Seq("foo").asJava) shouldBe true
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe true
    }

    "support lists" in {

      val schema = AvroSchema[List[String]]
      val typeGuard = TypeGuardedDecoding[List[String]].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(Seq("foo").asJava) shouldBe true
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe true
    }

    "support byte arrays" in {

      val schema = AvroSchema[Array[Byte]]
      val typeGuard = TypeGuardedDecoding[Array[Byte]].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
      typeGuard.isDefinedAt(Array[Byte](1, 2)) shouldBe true
      typeGuard.isDefinedAt(ByteBuffer.wrap(Array[Byte](1, 2))) shouldBe true
    }

    "support bytebuffers" in {

      val schema = AvroSchema[ByteBuffer]
      val typeGuard = TypeGuardedDecoding[ByteBuffer].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe false
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
      typeGuard.isDefinedAt(Array[Byte](1, 2)) shouldBe true
      typeGuard.isDefinedAt(ByteBuffer.wrap(Array[Byte](1, 2))) shouldBe true
    }

    "support uuids" in {

      val schema = AvroSchema[UUID]
      val typeGuard = TypeGuardedDecoding[UUID].guard(schema)

      typeGuard.isDefinedAt("foo") shouldBe true
      typeGuard.isDefinedAt(Utf8("hello")) shouldBe true
      typeGuard.isDefinedAt(false) shouldBe false
      typeGuard.isDefinedAt(123) shouldBe false
      typeGuard.isDefinedAt(12.34) shouldBe false
      typeGuard.isDefinedAt(Map("a" -> "b").asJava) shouldBe false
      typeGuard.isDefinedAt(List("foo").asJava) shouldBe false
    }
  }
}
