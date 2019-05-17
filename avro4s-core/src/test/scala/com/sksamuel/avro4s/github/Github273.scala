package com.sksamuel.avro4s.github

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
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
    implicitly[SchemaFor[DeeplyNested]].schema
    implicitly[SchemaFor[MixedClassesAndObjects]].schema
    implicitly[Encoder[Coproducts]]
    implicitly[Encoder[CoproductOfCoproductsField]]
    implicitly[Encoder[SealedTraitOfSealedTrait]]
    implicitly[Encoder[DeeplyNested]]
    implicitly[Encoder[MixedClassesAndObjects]]
    implicitly[Decoder[Coproducts]]
    implicitly[Decoder[CoproductOfCoproductsField]]
    implicitly[Decoder[SealedTraitOfSealedTrait]]
    implicitly[Decoder[MixedClassesAndObjects]]
    implicitly[Decoder[DeeplyNested]]
  }

}

object Github273 {
  case class Coproducts(cp: Int :+: String :+: Boolean :+: CNil)
  @SuppressWarnings(Array())
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

  sealed trait MixedClassesAndObjects

  object MixedClassesAndObjects {
    final case object CaseObject
      extends MixedClassesAndObjects

    final case class CaseClass(x: Int)
      extends MixedClassesAndObjects

    final case class CaseClassEmptyParams()
      extends MixedClassesAndObjects
  }

  sealed trait DeeplyNested

  object DeeplyNested {
    final case class One(x: Int, y: One.Nested)
      extends DeeplyNested

    object One {
      sealed trait Nested

      object Nested {
        //final case class CaseObject()
          //extends Nested
        final case class CaseClass(x: Int)
          extends Nested
      }
    }
  }
}
