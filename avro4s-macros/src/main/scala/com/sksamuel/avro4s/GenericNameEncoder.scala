package com.sksamuel.avro4s

import scala.reflect.macros.whitebox

object GenericNameEncoder {

  import scala.reflect.runtime.universe

  def apply(tpe: universe.Type): String = {
    val name = tpe.typeSymbol.name.decodedName.toString
    tpe.typeArgs match {
      case Nil => name
      case args => name + "__" + args.map(apply).mkString("_")
    }
  }

  def apply[C <: whitebox.Context](c: C)(tpe: c.Type): String = {
    val name = tpe.typeSymbol.name.decodedName.toString
    tpe.typeArgs match {
      case Nil => name
      case args => name + "__" + args.map(apply(c)(_)).mkString("_")
    }
  }
}
