package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.UseFieldMapper
import magnolia.{CaseClass, Magnolia, SealedTrait}

import scala.language.experimental.macros
import scala.reflect.runtime.universe._

trait MagnoliaGeneratedEncoders {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  type Typeclass[T] = EncoderV2[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): EncoderV2[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.encoder(ctx, UseFieldMapper(fieldMapper))
      case SealedTraitShape.ScalaEnum => ScalaEnums.encoder(ctx)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): EncoderV2[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => Records.encoder(ctx, UseFieldMapper(fieldMapper))
      case CaseClassShape.ValueType => ValueTypes.encoder(ctx, UseFieldMapper(fieldMapper))
    }
}

trait MagnoliaGeneratedDecoders {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  type Typeclass[T] = DecoderV2[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): DecoderV2[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.decoder(ctx, UseFieldMapper(fieldMapper))
      case SealedTraitShape.ScalaEnum => ScalaEnums.decoder(ctx)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): DecoderV2[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => Records.decoder(ctx, UseFieldMapper(fieldMapper))
      case CaseClassShape.ValueType => ValueTypes.decoder(ctx, UseFieldMapper(fieldMapper))
    }
}

trait MagnoliaGeneratedCodecs {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  type Typeclass[T] = Codec[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): Codec[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.codec(ctx, UseFieldMapper(fieldMapper))
      case SealedTraitShape.ScalaEnum => ScalaEnums.codec(ctx)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): Codec[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => Records.codec(ctx, UseFieldMapper(fieldMapper))
      case CaseClassShape.ValueType => ValueTypes.codec(ctx, UseFieldMapper(fieldMapper))
    }
}
