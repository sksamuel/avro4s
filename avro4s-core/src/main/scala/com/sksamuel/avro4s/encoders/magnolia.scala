package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.typeutils.{CaseClassShape, DatatypeShape, SealedTraitShape}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import magnolia1.{CaseClass, AutoDerivation, SealedTrait, TypeInfo}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.deriving.Mirror

trait MagnoliaDerivedEncoder extends AutoDerivation[Encoder] :
  override def join[T](ctx: CaseClass[Encoder, T]): Encoder[T] = new RecordEncoder(ctx)

  override def split[T](ctx: SealedTrait[Encoder, T]): Encoder[T] =
    DatatypeShape.of[T](ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.encoder(ctx)
      case SealedTraitShape.Enum => SealedTraits.encoder(ctx)
    }