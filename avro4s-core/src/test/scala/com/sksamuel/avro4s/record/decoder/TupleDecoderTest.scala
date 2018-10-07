package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class TupleDecoderTest extends FunSuite with Matchers {

  case class Test2(z: (String, Int))
  case class Test3(z: (String, Int, Long))
  case class Test4(z: (String, Int, Boolean, Double))
  case class Test5(z: (String, Int, String, Boolean, String))

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
  }

  test("decode tuple4") {
  }

  test("decode tuple5") {
  }
}


