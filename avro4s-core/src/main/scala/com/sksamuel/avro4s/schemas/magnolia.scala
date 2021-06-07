package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.typeutils.{CaseClassShape, DatatypeShape, SealedTraitShape}
import com.sksamuel.avro4s.{SchemaConfiguration, SchemaFor}
import magnolia.{CaseClass, Derivation, SealedTrait, TypeInfo}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.deriving.Mirror

trait MagnoliaDerivedSchemas extends Derivation[SchemaFor] :

  def join[T](ctx: CaseClass[SchemaFor, T]): SchemaFor[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record => Records.schema(ctx)
      case CaseClassShape.ValueType => ???
    }

  override def split[T](ctx: SealedTrait[SchemaFor, T]): SchemaFor[T] =
    DatatypeShape.of[T](ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.schema(ctx)
      case SealedTraitShape.ScalaEnum => ??? // SchemaFor[T](ScalaEnums.schema(ctx), DefaultFieldMapper)}
    }