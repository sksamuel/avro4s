package com.sksamuel.avro4s.record.decoder

import java.util.UUID

import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultNamingStrategy}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

class UUIDDecoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Decoder" should {
    "decode uuids" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchema[UUIDTest]
      val record = new GenericData.Record(schema)
      record.put("uuid", uuid.toString)
      Decoder[UUIDTest].decode(record, schema, DefaultNamingStrategy) shouldBe UUIDTest(uuid)
    }
    "decode UUIDSs encoded as Utf8" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchema[UUIDTest]
      val record = new GenericData.Record(schema)
      record.put("uuid", new Utf8(uuid.toString))
      Decoder[UUIDTest].decode(record, schema, DefaultNamingStrategy) shouldBe UUIDTest(uuid)
    }
    "decode seq of uuids" in {
      val schema = AvroSchema[UUIDSeq]

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()

      val record = new GenericData.Record(schema)
      record.put("uuids", List(uuid1.toString, uuid2.toString).asJava)

      Decoder[UUIDSeq].decode(record, schema, DefaultNamingStrategy) shouldBe UUIDSeq(List(uuid1, uuid2))
    }
    "decode Option[UUID]" in {
      val schema = AvroSchema[UUIDOption]

      val uuid = UUID.randomUUID()
      val record1 = new GenericData.Record(schema)
      record1.put("uuid", uuid.toString)

      Decoder[UUIDOption].decode(record1, schema, DefaultNamingStrategy) shouldBe UUIDOption(Some(uuid))

      val record2 = new GenericData.Record(schema)
      record2.put("uuid", null)

      Decoder[UUIDOption].decode(record2, schema, DefaultNamingStrategy) shouldBe UUIDOption(None)
    }
  }
}

case class UUIDTest(uuid: UUID)
case class UUIDSeq(uuids: Seq[UUID])
case class UUIDOption(uuid: Option[UUID])
