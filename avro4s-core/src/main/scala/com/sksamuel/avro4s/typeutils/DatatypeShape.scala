package com.sksamuel.avro4s.typeutils

import com.sksamuel.avro4s.SchemaFor
import magnolia.{CaseClass, SealedTrait}

enum CaseClassShape:
  case ValueType, Record

enum SealedTraitShape:
  case TypeUnion, ScalaEnum

object DatatypeShape:

  def of[T](ctx: SealedTrait[SchemaFor, T]): SealedTraitShape = SealedTraitShape.TypeUnion
  //    if(allSubtypesAreObjects) SealedTraitShape.ScalaEnum else SealedTraitShape.TypeUnion

  def of[Typeclass[_], T](ctx: CaseClass[Typeclass, T]): CaseClassShape =
    if (ctx.isValueClass) CaseClassShape.ValueType else CaseClassShape.Record