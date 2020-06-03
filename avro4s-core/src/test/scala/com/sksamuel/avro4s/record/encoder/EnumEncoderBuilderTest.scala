package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.Encoder
import org.apache.avro.generic.GenericEnumSymbol
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EnumEncoderBuilderTest extends AnyFunSuite with Matchers {

  case class CaseClassFourFields(a: String, b: Boolean, c: Int, d: Double)

  sealed trait People
  object People {
    case object Adam extends People
    case object Barry extends People
    case object Charlie extends People
  }

  test("Encoder.record happy path") {

    val encoder = Encoder.enum[People](
      name = "myrecord",
      namespace = "a.b.c",
      symbols = List("adam", "barry", "charlie")
    ) {
      case People.Adam => "adam"
      case People.Barry => "barry"
      case People.Charlie => "charlie"
    }

    encoder.encode(People.Adam).asInstanceOf[GenericEnumSymbol[_]].toString shouldBe "adam"
    encoder.encode(People.Barry).asInstanceOf[GenericEnumSymbol[_]].toString shouldBe "barry"
    encoder.encode(People.Charlie).asInstanceOf[GenericEnumSymbol[_]].toString shouldBe "charlie"
  }
}
