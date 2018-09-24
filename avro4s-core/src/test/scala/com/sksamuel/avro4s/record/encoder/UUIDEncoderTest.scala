package com.sksamuel.avro4s.record.encoder

import java.util.UUID

import com.sksamuel.avro4s.internal.{Encoder, InternalRecord, AvroSchema}
import org.scalatest.{Matchers, WordSpec}

class UUIDEncoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Encoder" should {
    "encode uuids" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchema[UUIDTest]
      Encoder[UUIDTest].encode(UUIDTest(uuid), schema) shouldBe InternalRecord(schema, Vector(uuid.toString))
    }
    "encode seq of uuids" in {
      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val schema = AvroSchema[UUIDSeq]
      Encoder[UUIDSeq].encode(UUIDSeq(Seq(uuid1, uuid2)), schema) shouldBe InternalRecord(schema, Vector(List(uuid1.toString, uuid2.toString).asJava))
    }
    "encode UUIDs with defaults" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchema[UUIDDefault]
      Encoder[UUIDDefault].encode(UUIDDefault(uuid), schema) shouldBe InternalRecord(schema, Vector(uuid.toString))
    }
    "encode Option[UUID]" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchema[UUIDOption]
      Encoder[UUIDOption].encode(UUIDOption(Some(uuid)), schema) shouldBe InternalRecord(schema, Vector(uuid.toString))
      Encoder[UUIDOption].encode(UUIDOption(None), schema) shouldBe InternalRecord(schema, Vector(null))
    }
  }
}

case class UUIDTest(uuid: UUID)
case class UUIDSeq(uuids: Seq[UUID])
case class UUIDDefault(uuid: UUID = UUID.fromString("86da265c-95bd-443c-8860-9381efca059d"))
case class UUIDOption(uuid: Option[UUID])