package com.sksamuel.avro4s

import scala.reflect.macros.whitebox

class TypeHelper[C <: whitebox.Context](val c: C) {

  import c.universe._

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

  def primaryConstructorParams(tpe: c.universe.Type): List[TypeSymbol] = {
    val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
    val defaultConstructor = {
      if (constructorSymbol.isMethod) constructorSymbol.asMethod
      else {
        val ctors = constructorSymbol.asTerm.alternatives
        ctors.map(_.asMethod).find(_.isPrimaryConstructor).get
      }
    }

    defaultConstructor.paramLists.reduceLeft(_ ++ _).map(_.asType)
  }

  def findAnnotation[A <: scala.annotation.Annotation, B](sym: c.Symbol)(f: PartialFunction[List[Tree], B]): Option[B] = {
    sym.annotations.collectFirst {
      case anno if anno.tree.tpe <:< c.weakTypeOf[A] => f(anno.tree.children.tail)
    }
  }

  def fixed(sym: c.Symbol): Option[AvroFixed] = findAnnotation[AvroFixed, AvroFixed](sym) {
    case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
  }
}

object TypeHelper {
  def apply[C <: whitebox.Context](c: C): TypeHelper[c.type] = new TypeHelper[c.type](c)
}
