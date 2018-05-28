package com.sksamuel.avro4s

import java.nio.file.Files
import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}

class UUIDTest extends FlatSpec with Matchers {

  case class UUIDSeq(uuids: Seq[UUID])
  case class UUIDDefault(decimal: UUID = UUID.fromString("86da265c-95bd-443c-8860-9381efca059d"))
  case class UUIDTest(uuid: UUID)
  case class UUIDOption(uuid: Option[UUID])

  "UUID" should "be represented as a logical type on Strings" in {
    val schema = SchemaFor[UUIDTest]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid.json"))
    schema shouldBe expected
  }

  it should "be serializable" in {

    val file = Files.createTempFile("UUID", ".avro")

    val a = UUIDTest(UUID.randomUUID())
    val b = UUIDTest(UUID.randomUUID())

    val out = AvroOutputStream.data[UUIDTest](file)
    out.write(Seq(a, b))
    out.close()

    val in = AvroInputStream.data[UUIDTest](file)
    in.iterator.toList shouldBe List(a, b)
    in.close()
  }

  "Option[UUID]" should "be represented as a union" in {
    val schema = SchemaFor[UUIDOption]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid_option.json"))
    schema shouldBe expected
  }

  it should "be serializable" in {

    val file = Files.createTempFile("uuid_option", ".avro")

    val a = UUIDOption(Some(UUID.randomUUID()))
    val b = UUIDOption(None)
    val c = UUIDOption(Some(UUID.randomUUID()))

    val out = AvroOutputStream.data[UUIDOption](file)
    out.write(List(a, b, c))
    out.close()

    val in = AvroInputStream.data[UUIDOption](file)
    in.iterator.toList shouldBe List(a, b, c)
    in.close()
  }

  "UUID with default value" should "be represented as a logical type with default" in {
    val schema = SchemaFor[UUIDDefault]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid_default.json"))
    schema shouldBe expected
  }

  it should "be serializable" in {

    val file = Files.createTempFile("uuid_default", ".avro")

    val a = UUIDDefault(UUID.randomUUID)
    val b = UUIDDefault()

    val out = AvroOutputStream.data[UUIDDefault](file)
    out.write(List(a, b))
    out.close()

    val in = AvroInputStream.data[UUIDDefault](file)
    in.iterator.toList shouldBe List(a, b)
    in.close()
  }

  "Seq[UUID]" should "be represented as an array of logical types" in {
    val schema = SchemaFor[UUIDSeq]()
    val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/uuid_seq.json"))
    schema shouldBe expected
  }

  it should "be serializable" in {

    val file = Files.createTempFile("uuid_seq", ".avro")

    val a = UUIDSeq(Seq(UUID.randomUUID, UUID.randomUUID))
    val b = UUIDSeq(Seq(UUID.randomUUID, UUID.randomUUID))

    val out = AvroOutputStream.data[UUIDSeq](file)
    out.write(List(a, b))
    out.close()

    val in = AvroInputStream.data[UUIDSeq](file)
    in.iterator.toList shouldBe List(a, b)
    in.close()
  }
}
