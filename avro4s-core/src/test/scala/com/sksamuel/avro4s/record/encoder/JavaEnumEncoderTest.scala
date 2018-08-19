package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, SchemaEncoder}
import com.sksamuel.avro4s.schema.Wine
import org.scalatest.{Matchers, WordSpec}

class JavaEnumEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode java enums" in {
      val schema = SchemaEncoder[JavaEnum].encode()
      Encoder[JavaEnum].encode(JavaEnum(Wine.Malbec), schema) shouldBe InternalRecord(schema, Vector(Wine.Malbec))
    }
  }
}

case class JavaEnum(wine: Wine)