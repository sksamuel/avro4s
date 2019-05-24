package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultNamingStrategy}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class TupleDecoderTest extends FunSuite with Matchers {

  case class Test2(z: (String, Int))
  case class Test3(z: (String, Int, Boolean))
  case class Test4(z: (String, Int, Boolean, Double))
  case class Test5(z: (String, Int, Boolean, Double, Long))

  test("decode tuple2") {
    val schema = AvroSchema[Test2]
    val z = new GenericData.Record(AvroSchema[(String, Int)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test2].decode(record, schema) shouldBe Test2(("hello", 214))
  }

  test("decode tuple3") {
    val schema = AvroSchema[Test3]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test3].decode(record, schema) shouldBe Test3(("hello", 214, true))
  }

  test("decode tuple4") {
    val schema = AvroSchema[Test4]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean, Double)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    z.put("_4", java.lang.Double.valueOf(56.45))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test4].decode(record, schema) shouldBe Test4(("hello", 214, true, 56.45))
  }

  test("decode tuple5") {
    val schema = AvroSchema[Test5]
    val z = new GenericData.Record(AvroSchema[(String, Int, Boolean, Double, Long)])
    z.put("_1", new Utf8("hello"))
    z.put("_2", java.lang.Integer.valueOf(214))
    z.put("_3", java.lang.Boolean.valueOf(true))
    z.put("_4", java.lang.Double.valueOf(56.45))
    z.put("_5", java.lang.Long.valueOf(9999999999L))
    val record = new GenericData.Record(schema)
    record.put("z", z)
    Decoder[Test5].decode(record, schema) shouldBe Test5(("hello", 214, true, 56.45, 9999999999L))
  }
}


