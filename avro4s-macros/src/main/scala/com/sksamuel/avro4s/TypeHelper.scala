package com.sksamuel.avro4s

import scala.reflect.macros.whitebox.Context

class TypeHelper[C <: Context](val c: C) {
  def fieldsOf(tpe: c.universe.Type) = {
    import c.universe._
    val primaryConstructorOpt = tpe.members.collectFirst {
      case method: MethodSymbolApi if method.isPrimaryConstructor => method
    }

    primaryConstructorOpt.map { constructor =>
      val constructorTypeContext = constructor.typeSignatureIn(tpe).dealias
      val constructorArguments = constructorTypeContext.paramLists
      constructorArguments.headOption.map { symbols =>
        symbols.map(s => s -> s.typeSignatureIn(constructorTypeContext).dealias)
      }.getOrElse(Nil)
    }.getOrElse(Nil)
  }
}

object TypeHelper {
  def apply[C <: Context](c: C): TypeHelper[c.type] = new TypeHelper[c.type](c)
}
