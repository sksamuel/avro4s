package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{Decoder, Encoder, RecordFormat, SchemaFor}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.scalatest.{FunSuite, Matchers}
import shapeless.{:+:, CNil}

class Github273 extends FunSuite with Matchers {
  import Github273._

  test("Generating typeclass instances for nested coproduct types should work") {

    implicitly[SchemaFor[Coproducts]].schema
    implicitly[SchemaFor[CoproductOfCoproductsField]].schema
    implicitly[SchemaFor[SealedTraitOfSealedTrait]].schema
    implicitly[Encoder[Coproducts]]
    implicitly[Encoder[CoproductOfCoproductsField]]
    implicitly[Encoder[SealedTraitOfSealedTrait]]
    implicitly[Decoder[Coproducts]]
    implicitly[Decoder[CoproductOfCoproductsField]]
    implicitly[Decoder[SealedTraitOfSealedTrait]]
  }

}

object Github273 {
  case class Coproducts(cp: Int :+: String :+: Boolean :+: CNil)
  case class CoproductOfCoproductsField(cp: Coproducts :+: Boolean :+: CNil)

  sealed trait SealedTraitOfSealedTrait

  object SealedTraitOfSealedTrait {
    final case class X(x: SealedTrait)
      extends SealedTraitOfSealedTrait

    sealed trait SealedTrait

    object SealedTrait {
      final case class Y(x: Int)
        extends SealedTrait
    }
  }

}
