package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TupleEncoderTest extends AnyFunSuite with Matchers {

  test("encode tuple2") {
    case class Test(z: (String, Option[Int]))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(schema).apply(Test("hello", Some(55))).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1").asInstanceOf[Utf8] shouldBe new Utf8("hello")
    z.get("_2").asInstanceOf[Int] shouldBe 55
  }

  test("encode tuple3") {
    case class Test(z: (String, Option[Int], Long))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(schema).apply(Test("hello", Some(55), 9999999L)).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1").asInstanceOf[Utf8] shouldBe new Utf8("hello")
    z.get("_2").asInstanceOf[Int] shouldBe 55
    z.get("_3").asInstanceOf[Long] shouldBe 9999999L
  }

  test("encode tuple4") {
    case class Test(z: (String, Option[Int], Boolean, Double))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(schema).apply(Test("hello", Some(55), true, 0.24)).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1").asInstanceOf[Utf8] shouldBe new Utf8("hello")
    z.get("_2").asInstanceOf[Int] shouldBe 55
    z.get("_3").asInstanceOf[Boolean] shouldBe true
    z.get("_4").asInstanceOf[Double] shouldBe 0.24
  }

  test("encode tuple5") {
    case class Test(z: (String, Option[Int], String, Boolean, String))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(schema).apply(Test("a", Some(55), "b", true, "c")).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1").asInstanceOf[Utf8] shouldBe new Utf8("a")
    z.get("_2").asInstanceOf[Int] shouldBe 55
    z.get("_3").asInstanceOf[Utf8] shouldBe new Utf8("b")
    z.get("_4").asInstanceOf[Boolean] shouldBe true
    z.get("_5").asInstanceOf[Utf8] shouldBe new Utf8("c")
  }
}


