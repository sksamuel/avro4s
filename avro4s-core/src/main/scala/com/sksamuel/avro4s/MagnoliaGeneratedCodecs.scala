package com.sksamuel.avro4s

import magnolia.{CaseClass, Magnolia, SealedTrait}
import scala.reflect.runtime.universe._
import scala.language.experimental.macros

trait MagnoliaGeneratedCodecs {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  def apply[T](implicit codec: Codec[T]): Codec[T] = codec

  type Typeclass[T] = Codec[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): Codec[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnionCodec(ctx)
      case SealedTraitShape.ScalaEnum => ScalaEnumCodec(ctx)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper = DefaultFieldMapper): Codec[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => RecordCodec(ctx, fieldMapper)
      case CaseClassShape.ValueType => ValueTypeCodec(ctx)
    }
}
