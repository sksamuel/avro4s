package com.sksamuel.avro4s

import magnolia.{CaseClass, SealedTrait}

import scala.reflect.runtime.universe._

object DatatypeShape {
  def of[TC[_], T: WeakTypeTag](ctx: SealedTrait[TC, T]): SealedTraitShape = {
    import scala.reflect.runtime.universe

    val runtimeMirror = universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
    val tpe = runtimeMirror.weakTypeOf[T]
    val allSubtypesAreObjects = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.knownDirectSubclasses.forall(_.isModuleClass)

    if(allSubtypesAreObjects) SealedTraitShape.ScalaEnum else SealedTraitShape.TypeUnion
  }

  def of[TC[_], T](ctx: CaseClass[TC, T]): CaseClassShape =
    if(ctx.isValueClass) CaseClassShape.ValueType else CaseClassShape.Record
}

sealed trait CaseClassShape

object CaseClassShape {
  case object ValueType extends CaseClassShape
  case object Record extends CaseClassShape
}

sealed trait SealedTraitShape

object SealedTraitShape {
  case object TypeUnion extends SealedTraitShape
  case object ScalaEnum extends SealedTraitShape
}
