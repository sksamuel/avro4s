package com.sksamuel.avro4s

import scala.language.experimental.macros
import scala.reflect.macros.Context

//object Macros {
//
//  import scala.reflect.macros.Context
//
//  def toMap_impl[T: c.WeakTypeTag](c: Context) = {
//    import c.universe._
//
//    val mapApply = Select(reify(Map).tree, newTermName("apply"))
//
//    val pairs = weakTypeOf[T].declarations.collect {
//      case m: MethodSymbol if m.isCaseAccessor =>
//        val name = c.literal(m.name.decoded)
//        val value = c.Expr(Select(c.resetAllAttrs(c.prefix.tree), m.name))
//        reify(name.splice -> value.splice).tree
//    }
//
//    c.Expr[Map[String, Any]](Apply(mapApply, pairs.toList))
//  }
//}

trait Schemas[T] {
  def s: String
}

object Macros {
  implicit def materializeSchema[T]: Schemas[T] = macro materializeMappableImpl[T]

  def materializeMappableImpl[T: c.WeakTypeTag](c: Context): c.Expr[Schemas[T]] = {

    import c.universe._
    val tpe = weakTypeOf[T]

    println(tpe)

    c.Expr[Schemas[T]] { q"""
      new Schemas[$tpe] {
        def s: String = "sammy"
      }
    """
    }
  }
}