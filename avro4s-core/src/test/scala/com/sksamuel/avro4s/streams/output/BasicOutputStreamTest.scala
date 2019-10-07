package com.sksamuel.avro4s.streams.output

import com.sksamuel.avro4s.{Encoder, SchemaFor}
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.avro.util.Utf8

class BasicOutputStreamTest extends OutputStreamTest {

  test("write out booleans") {
    case class Test(z: Boolean)
    writeRead(Test(true)) { record =>
      record.get("z") shouldBe true
    }
  }

  test("write out strings") {
    case class Test(z: String)
    writeRead(Test("Hello world")) { record =>
      record.get("z") shouldBe new Utf8("Hello world")
    }
  }

  test("write out longs") {
    case class Test(z: Long)
    writeRead(Test(65653L)) { record =>
      record.get("z") shouldBe 65653L
    }
  }

  test("write out ints") {
    case class Test(z: Int)
    writeRead(Test(44)) { record =>
      record.get("z") shouldBe 44
    }
  }

  test("write out doubles") {
    case class Test(z: Double)
    writeRead(Test(3.235)) { record =>
      record.get("z") shouldBe 3.235
    }
  }

  test("write out floats") {
    case class Test(z: Float)
    writeRead(Test(3.4F)) { record =>
      record.get("z") shouldBe 3.4F
    }
  }

  test("write out generic record") {
    val schema = new Parser().parse(
      """{"type":"record","name":"Test","fields":[{"name":"field","type":"string"}]}"""
    )

    implicit val encoder: Encoder[GenericRecord] = (r, _, _) => r
    implicit val schemaFor: SchemaFor[GenericRecord] = _ => schema

    val record: GenericRecord =
      new GenericRecordBuilder(schema).set("field", "value").build()

    writeRead(record) { rec =>
      rec.get("field") shouldBe new Utf8("value")
    }
  }
}
