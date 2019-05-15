package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{Decoder, Encoder, RecordFormat, SchemaFor}
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FunSuite}
import shapeless.{:+:, CNil}

object Github273 extends FunSuite {

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

  implicitly[SchemaFor[Coproducts]]
  implicitly[SchemaFor[CoproductOfCoproductsField]]
  implicitly[SchemaFor[SealedTraitOfSealedTrait]]
  implicitly[Encoder[Coproducts]]
  implicitly[Encoder[CoproductOfCoproductsField]]
  implicitly[Encoder[SealedTraitOfSealedTrait]]
  implicitly[Decoder[Coproducts]]
  implicitly[Decoder[CoproductOfCoproductsField]]
  implicitly[Decoder[SealedTraitOfSealedTrait]]
}
