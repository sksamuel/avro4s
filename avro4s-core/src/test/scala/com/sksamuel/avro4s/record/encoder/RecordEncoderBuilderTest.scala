package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{Encoder, EncoderField, Recursive}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RecordEncoderBuilderTest extends AnyFunSuite with Matchers {

  case class CaseClassFourFields(a: String, b: Boolean, c: Int, d: Double)
  case class CaseClassOneField(a: String)
  case class CaseClassWithNestedCaseClass(a: String, b: CaseClassOneField)

  test("Encoder.record happy path") {

    import Encoder.field

    val encoder = Encoder.record[CaseClassFourFields](
      name = "myrecord",
      namespace = "a.b.c"
    ) {
      List(
        field("a", _.a),
        field("b", _.b),
        field("c", _.c),
        field("d", _.d)
      )
    }

    val record = encoder.encode(CaseClassFourFields("foo", true, 123, 99.88)).asInstanceOf[GenericRecord]
    record.get("a") shouldBe new Utf8("foo")
    record.get("b") shouldBe true
    record.get("c") shouldBe 123
    record.get("d") shouldBe 99.88
  }

  test("Encoder.record with nested case classes") {
    import Encoder.field
    val encoder = Encoder.record[CaseClassWithNestedCaseClass](
      name = "myrecord",
      namespace = "a.b.c"
    ) {
      List(
        field("a", _.a),
        field("b", _.b)
      )
    }

    val record = encoder.encode(CaseClassWithNestedCaseClass("foo", CaseClassOneField("bar"))).asInstanceOf[GenericRecord]
    record.get("a") shouldBe new Utf8("foo")
    record.get("b").asInstanceOf[GenericRecord].get("a") shouldBe new Utf8("bar")
  }

  test("Encoder.record with recursive types") {
    import Encoder.field

    implicit def branchEncoder: Encoder[Recursive.Branch[Int]] = Encoder.record[Recursive.Branch[Int]](
      name = "IntBranch",
      namespace = "com.sksamuel.avro4s.Recursive"
    ) {
      List(
        field("right", _.right),
        field("left", _.left)
      )
    }

    Encoder[Recursive.Tree[Int]].schema shouldBe expectedSchema("/recursive_custom_record.json")
  }

  def expectedSchema(name: String) =
    new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(name))

}