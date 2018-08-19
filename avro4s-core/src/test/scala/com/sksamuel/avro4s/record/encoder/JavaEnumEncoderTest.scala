package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{InternalRecord, RecordEncoder, SchemaEncoder}
import com.sksamuel.avro4s.schema.Wine
import org.scalatest.{Matchers, WordSpec}

class JavaEnumEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode java enums" in {
      val schema = SchemaEncoder[JavaEnum].encode()
      RecordEncoder[JavaEnum](schema).encode(JavaEnum(Wine.Malbec)) shouldBe InternalRecord(schema, Vector(Wine.Malbec))
    }
  }
}

case class JavaEnum(wine: Wine)