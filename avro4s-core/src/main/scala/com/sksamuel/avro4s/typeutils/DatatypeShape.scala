package com.sksamuel.avro4s.typeutils

import com.sksamuel.avro4s.SchemaFor
import magnolia1.{CaseClass, SealedTrait}

enum CaseClassShape:
  case ValueType, Record

enum SealedTraitShape:
  case TypeUnion, Enum

object DatatypeShape:

  def of[T](ctx: SealedTrait[_, T]): SealedTraitShape = {
    val allSubtypesAreObjects = ctx.subtypes.forall(_.isObject)
    if (ctx.isEnum || allSubtypesAreObjects) SealedTraitShape.Enum else SealedTraitShape.TypeUnion
  }

  def of[Typeclass[_], T](ctx: CaseClass[Typeclass, T]): CaseClassShape =
    if (ctx.isValueClass) CaseClassShape.ValueType else CaseClassShape.Record