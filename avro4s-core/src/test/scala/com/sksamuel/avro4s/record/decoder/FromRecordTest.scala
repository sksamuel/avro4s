package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

case class HasSomeFields(str: String, int: Int, boolean: Boolean, nested: Nested)
case class Nested(foo: String)
case class HasLessFields(str: String, boolean: Boolean, nested: Nested)

class FromRecordTest extends WordSpec with Matchers {

  "FromRecord" should {
    "decode to class with a subset of fields used to encode" in {
      val schema = AvroSchema[HasSomeFields]
      val nestedSchema = AvroSchema[Nested]

      val record = new GenericData.Record(schema)
      val nestedRecord = new GenericData.Record(nestedSchema)
      record.put("str", "hello")
      record.put("int", 42)
      record.put("boolean", false)
      nestedRecord.put("foo", "there")
      record.put("nested", nestedRecord)

      FromRecord[HasLessFields].from(record) shouldBe HasLessFields("hello", false, Nested("there"))
    }
  }
}


