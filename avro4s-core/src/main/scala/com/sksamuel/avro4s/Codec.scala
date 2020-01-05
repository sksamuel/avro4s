package com.sksamuel.avro4s

import magnolia.{CaseClass, Magnolia, SealedTrait}
import org.apache.avro.Schema

import scala.annotation.implicitNotFound
import scala.reflect.runtime.universe._
import scala.language.experimental.macros

@implicitNotFound(msg = """Unable to build a codec; please check the following:
- an implicit com.sksamuel.avro4s.FieldMapper must be in scope, and
- the given type must be either a case class or a sealed trait with subtypes being
  case classes or case objects.""")
trait Codec[T] {
  self =>

  def schema: Schema

  def encode(value: T): AnyRef

  def decode(value: Any): T

  def withSchema(schema: Schema): Codec[T] = {
    val s = schema
    new Codec[T] {
      val schema = s

      def encode(value: T): AnyRef = self.encode(value)

      def decode(value: Any): T = self.decode(value)
    }
  }
}

object Codec extends BaseCodecs {

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  def apply[T](implicit codec: Codec[T]): Codec[T] = codec

  type Typeclass[T] = Codec[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(implicit fieldMapper: FieldMapper): Codec[T] = {
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => new TypeUnionCodec(ctx)

      case SealedTraitShape.ScalaEnum => ???
    }
  }

  def combine[T: WeakTypeTag](ctx: CaseClass[Typeclass, T])(implicit fieldMapper: FieldMapper): Codec[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record => RecordCodec(ctx, fieldMapper)

      case CaseClassShape.ValueType => ???
    }
}
