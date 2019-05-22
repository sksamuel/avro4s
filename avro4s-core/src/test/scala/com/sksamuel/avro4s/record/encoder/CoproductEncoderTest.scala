package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.{AvroSchema, DefaultNamingStrategy, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}
import shapeless.{:+:, CNil, Coproduct}

class CoproductEncoderTest extends FunSuite with Matchers {

  test("coproducts with primitives") {
    val schema = AvroSchema[CPWrapper]
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG](4)), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(4)))
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG]("wibble")), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(new Utf8("wibble")))
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG](true)), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(java.lang.Boolean.valueOf(true)))
  }

  test("coproducts with case classes") {
    val schema = AvroSchema[CPWrapper]
    val gschema = AvroSchema[Gimble]
    Encoder[CPWrapper].encode(CPWrapper(Coproduct[CPWrapper.ISBG](Gimble("foo"))), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(ImmutableRecord(gschema, Vector(new Utf8("foo")))))
  }

  test("options of coproducts") {
    val schema = AvroSchema[CPWithOption]
    Encoder[CPWithOption].encode(CPWithOption(Some(Coproduct[CPWrapper.ISBG]("foo"))), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(new Utf8("foo")))
    Encoder[CPWithOption].encode(CPWithOption(None), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(null))
  }

  test("coproducts with arrays") {
    val schema = AvroSchema[CPWithArray]
    Encoder[CPWithArray].encode(CPWithArray(Coproduct[CPWrapper.SSI](Seq("foo", "bar"))), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(java.util.Arrays.asList(new Utf8("foo"), new Utf8("bar"))))
    Encoder[CPWithArray].encode(CPWithArray(Coproduct[CPWrapper.SSI](4)), schema, DefaultNamingStrategy) shouldBe ImmutableRecord(schema, Vector(java.lang.Integer.valueOf(4)))
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
