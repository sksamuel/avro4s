package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroFixed, AvroSchema, Encoder, SchemaFor}
import org.apache.avro.generic.{GenericFixed, GenericRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

@AvroFixed(8)
case class QuarterSHA256(bytes: Array[Byte]) extends AnyVal

case class FixedString(@AvroFixed(7) mystring: String)

case class AvroMessage(q: QuarterSHA256, payload: Array[Byte])

@AvroFixed(8)
case class FixedValueType(z: String) extends AnyVal
case class OptionOfFixedValueTypes(opt: Option[FixedValueType])

class FixedEncoderTest extends AnyFunSuite with Matchers {

  val m = AvroMessage(
    QuarterSHA256(Array[Byte](0, 1, 2, 3, 4, 5, 6)),
    Array[Byte](0, 1, 2, 3)
  )

  // todo require value type support in magnolia for scala 3
  // test("encode fixed when used on a value type") {
  //   val schema = SchemaFor[AvroMessage]
  //   val record = Encoder[AvroMessage].encode(m).asInstanceOf[GenericRecord]
  //   record.get("q").asInstanceOf[GenericFixed].bytes().toVector shouldBe Vector(0, 1, 2, 3, 4, 5, 6, 0)
  // }

  test("encode fixed when used on a field in a case class") {
    val schema = AvroSchema[FixedString]
    val record = Encoder[FixedString].encode(schema).apply(FixedString("sam")).asInstanceOf[GenericRecord]
    record.get("mystring").asInstanceOf[GenericFixed].bytes.toVector shouldBe Vector(115, 97, 109, 0, 0, 0, 0)
  }

  // todo require value type support in magnolia for scala 3
  // test("support options of fixed value types") {
  //   val schema = AvroSchema[OptionOfFixedValueTypes]
  //   val record = Encoder[OptionOfFixedValueTypes].encode(schema).apply(OptionOfFixedValueTypes(Some(FixedValueType("sam")))).asInstanceOf[GenericRecord]
  //   record.get("opt").asInstanceOf[GenericFixed].bytes.toVector shouldBe Vector(115, 97, 109, 0, 0, 0, 0, 0)
  // }
}

