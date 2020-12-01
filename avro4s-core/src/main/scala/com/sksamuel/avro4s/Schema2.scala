package com.sksamuel.avro4s

import org.apache.avro.Schema
import scala.quoted._

trait Schema2 {
  def schema[T]: Schema
}

object Schema2 {
  
  inline def apply[T]: Unit = ${ applyImpl[T] }

  def applyImpl[T](using q: Quotes, tpe: Type[T]) = {
    import quotes.reflect._
    println("-----" + Type.show[T])
    val symbol = TypeTree.of[T].tpe.typeSymbol
    val classdef = symbol.tree.asInstanceOf[ClassDef]
    println("typeSymbol " + TypeTree.of[T].tpe.typeSymbol)
    println("fullName " + symbol.fullName)
    println("annots " + symbol.annots)
    println("fields " + symbol.fields)
    println("isClassDef " + symbol.isClassDef)
    println("caseFields " + symbol.caseFields)
    println("classdef " + classdef)
    println("termSymbol " + TypeTree.of[T].tpe.termSymbol)
    println("base " + TypeTree.of[T].tpe.baseClasses)
    println("class " + TypeTree.of[T].tpe.getClass)
    '{"hello"}
  }

  def natConstImpl(x: Expr[Int])(using Quotes): Expr[Int] = {
    import quotes.reflect._
    val xTree: Term = Term.of(x)
    xTree match {
       case _ =>
        report.error("Parameter must be a known constant")
        '{0}
    }
  }
}




