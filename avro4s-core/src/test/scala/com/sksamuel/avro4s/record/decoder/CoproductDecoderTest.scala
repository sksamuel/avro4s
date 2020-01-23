package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchemaV2, DecoderV2}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import shapeless.{:+:, CNil, Coproduct}

class CoproductDecoderTest extends AnyFunSuite with Matchers {

  test("coproducts with primitives") {
    val decoder = DecoderV2[CPWrapper]
    val record = new GenericData.Record(decoder.schema)
    record.put("u", new Utf8("wibble"))
    decoder.decode(record) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG]("wibble"))
  }

  test("coproducts with case classes") {
    val decoder = DecoderV2[CPWrapper]
    val gimble = new GenericData.Record(AvroSchemaV2[Gimble])
    gimble.put("x", new Utf8("foo"))
    val record = new GenericData.Record(decoder.schema)
    record.put("u", gimble)
    decoder.decode(record) shouldBe CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo")))
  }

  test("coproducts with options") {
    val codec = DecoderV2[CPWithOption]
    val gimble = new GenericData.Record(AvroSchemaV2[Gimble])
    gimble.put("x", new Utf8("foo"))
    val record = new GenericData.Record(codec.schema)
    record.put("u", gimble)
    codec.decode(record) shouldBe CPWithOption(Some(Coproduct[CPWrapper.ISBG](Gimble("foo"))))
  }
}

case class CPWithArray(u: CPWrapper.SSI)

case class Gimble(x: String)
case class CPWrapper(u: CPWrapper.ISBG)
case class CPWithOption(u: Option[CPWrapper.ISBG])

object CPWrapper {
  type ISBG = Int :+: String :+: Boolean :+: Gimble :+: CNil
  type SSI = Seq[String] :+: Int :+: CNil
}

case class Coproducts(union: Int :+: String :+: Boolean :+: CNil)
case class CoproductsOfCoproducts(union: (Int :+: String :+: CNil) :+: Boolean :+: CNil)
