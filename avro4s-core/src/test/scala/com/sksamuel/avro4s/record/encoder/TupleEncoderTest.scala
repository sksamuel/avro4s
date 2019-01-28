package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, Encoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class TupleEncoderTest extends FunSuite with Matchers {

  test("encode tuple2") {
    case class Test(z: (String, Option[Int]))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(Test("hello", Some(55)), schema).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1") shouldBe new Utf8("hello")
    z.get("_2") shouldBe 55
  }

  test("encode tuple3") {
    case class Test(z: (String, Option[Int], Long))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(Test("hello", Some(55), 9999999L), schema).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1") shouldBe new Utf8("hello")
    z.get("_2") shouldBe 55
    z.get("_3") shouldBe 9999999L
  }

  test("encode tuple4") {
    case class Test(z: (String, Option[Int], Boolean, Double))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(Test("hello", Some(55), true, 0.24), schema).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1") shouldBe new Utf8("hello")
    z.get("_2") shouldBe 55
    z.get("_3") shouldBe true
    z.get("_4") shouldBe 0.24
  }

  test("encode tuple5") {
    case class Test(z: (String, Option[Int], String, Boolean, String))
    val schema = AvroSchema[Test]
    val record = Encoder[Test].encode(Test("a", Some(55), "b", true, "c"), schema).asInstanceOf[GenericRecord]
    val z = record.get("z").asInstanceOf[GenericRecord]
    z.get("_1") shouldBe new Utf8("a")
    z.get("_2") shouldBe 55
    z.get("_3") shouldBe new Utf8("b")
    z.get("_4") shouldBe true
    z.get("_5") shouldBe new Utf8("c")
  }
}


