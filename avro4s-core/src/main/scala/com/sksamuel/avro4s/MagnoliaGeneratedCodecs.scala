package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.NoUpdate
import magnolia.{CaseClass, Magnolia, Param, SealedTrait}

import scala.reflect.runtime.universe._
import scala.language.experimental.macros

trait MagnoliaGeneratedEncoders {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  type Typeclass[T] = EncoderV2[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
    implicit fieldMapper: FieldMapper = DefaultFieldMapper): EncoderV2[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.encoder(ctx, NoUpdate)
      case SealedTraitShape.ScalaEnum => ScalaEnums.encoder(ctx)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): EncoderV2[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => Records.encoder(ctx, fieldMapper)
      case CaseClassShape.ValueType => ValueTypes.encoder(ctx, NoUpdate)
    }
}

trait MagnoliaGeneratedDecoders {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  type Typeclass[T] = DecoderV2[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
    implicit fieldMapper: FieldMapper = DefaultFieldMapper): DecoderV2[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.decoder(ctx, NoUpdate)
      case SealedTraitShape.ScalaEnum => ScalaEnums.decoder(ctx)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): DecoderV2[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => Records.decoder(ctx, fieldMapper)
      case CaseClassShape.ValueType => ValueTypes.decoder(ctx, NoUpdate)
    }
}

trait MagnoliaGeneratedCodecs {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  type Typeclass[T] = Codec[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): Codec[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.codec(ctx, NoUpdate)
      case SealedTraitShape.ScalaEnum => ScalaEnums.codec(ctx)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): Codec[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => Records.codec(ctx, fieldMapper)
      case CaseClassShape.ValueType => ValueTypes.codec(ctx, NoUpdate)
    }
}
