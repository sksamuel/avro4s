package com.sksamuel.avro4s

import scala.reflect.macros.whitebox

class TypeHelper[C <: whitebox.Context](val c: C) {

  import c.universe._

  def primaryConstructor(tpe: c.Type): Option[MethodSymbol] = {
    val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
    if (constructorSymbol.isMethod) Some(constructorSymbol.asMethod)
    else {
      val ctors = constructorSymbol.asTerm.alternatives
      ctors.map(_.asMethod).find(_.isPrimaryConstructor)
    }
  }

  def fieldsOf(tpe: c.universe.Type) = {
    val primaryConstructorOpt = primaryConstructor(tpe)

    primaryConstructorOpt.map { constructor =>
      val constructorTypeContext = constructor.typeSignatureIn(tpe).dealias
      val constructorArguments = constructorTypeContext.paramLists
      constructorArguments.headOption.map { symbols =>
        symbols.map(s => s -> s.typeSignatureIn(constructorTypeContext).dealias)
      }.getOrElse(Nil)
    }.getOrElse(Nil)
  }

  def primaryConstructorParams(tpe: c.Type): List[(c.Symbol, c.Type)] = {
    val constructorTypeContext = primaryConstructor(tpe).get.typeSignatureIn(tpe).dealias
    val constructorArguments = constructorTypeContext.paramLists.reduce(_ ++ _)
    constructorArguments.map { symbol =>
      symbol -> symbol.typeSignatureIn(constructorTypeContext).dealias
    }
  }

  def findAnnotation[A <: scala.annotation.Annotation, B](sym: c.Symbol)(f: PartialFunction[List[c.Tree], B]): Option[B] = {
    sym.annotations.collectFirst {
      case anno if anno.tree.tpe <:< c.weakTypeOf[A] => f(anno.tree.children.tail)
    }
  }

  def fixed(sym: c.Symbol): Option[AvroFixed] = sym.annotations.collectFirst {
    case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
      anno.tree.children.tail match {
        case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
      }
  }

  def avroName(sym: c.Symbol): Option[Tree] =
    sym
      .annotations
      .find(_.tree.tpe == typeOf[AvroName])
      .map(_.tree).map {
        case Apply(Select(New(_), _), List(name)) => name
        case _ =>
          c.abort(c.enclosingPosition, "Unexpected macro application")
      }
}

object TypeHelper {
  def apply[C <: whitebox.Context](c: C): TypeHelper[c.type] = new TypeHelper[c.type](c)
}
