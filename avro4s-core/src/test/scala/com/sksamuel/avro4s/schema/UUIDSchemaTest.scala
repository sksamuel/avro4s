package com.sksamuel.avro4s.schema

import java.util.UUID

import com.sksamuel.avro4s.internal.SchemaFor
import org.scalatest.{Matchers, WordSpec}

class UUIDSchemaTest extends WordSpec with Matchers {

  case class UUIDTest(uuid: UUID)
  case class UUIDSeq(uuids: Seq[UUID])
  case class UUIDDefault(uuid: UUID = UUID.fromString("86da265c-95bd-443c-8860-9381efca059d"))
  case class UUIDOption(uuid: Option[UUID])

  "SchemaEncoder" should {
    "support UUID logical types" in {
      val schema = SchemaFor[UUIDTest]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid.json"))
      schema shouldBe expected
    }
    "support Option[UUID] as a union" in {
      val schema = SchemaFor[UUIDOption]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid_option.json"))
      schema shouldBe expected
    }
    "support UUID with default value" in {
      val schema = SchemaFor[UUIDDefault]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid_default.json"))
      schema shouldBe expected
    }
    "support Seq[UUID] as an array of logical types" in {
      val schema = SchemaFor[UUIDSeq]
      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid_seq.json"))
      schema shouldBe expected
    }
  }
}
