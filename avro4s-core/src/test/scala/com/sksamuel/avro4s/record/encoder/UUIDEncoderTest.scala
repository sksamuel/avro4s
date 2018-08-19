package com.sksamuel.avro4s.record.encoder

import java.util.UUID

import com.sksamuel.avro4s.internal.{InternalRecord, RecordEncoder, SchemaEncoder}
import org.scalatest.{Matchers, WordSpec}

class UUIDEncoderTest extends WordSpec with Matchers {

  "RecordEncoder" should {
    "encode uuids" in {
      val uuid = UUID.randomUUID()
      val schema = SchemaEncoder[UUIDTest].encode()
      RecordEncoder[UUIDTest](schema).encode(UUIDTest(uuid)) shouldBe InternalRecord(schema, Vector(uuid))
    }
  }
}

case class UUIDTest(uuid: UUID)
