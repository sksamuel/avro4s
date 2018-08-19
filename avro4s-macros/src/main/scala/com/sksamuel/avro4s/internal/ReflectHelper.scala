package com.sksamuel.avro4s.internal

import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.macros.whitebox

class ReflectHelper[C <: whitebox.Context](val c: C) {

  import c.universe
  import c.universe._

  private val defswithsymbols = universe.asInstanceOf[Definitions with SymbolTable with StdNames]

  def primaryConstructor(tpe: c.Type): Option[MethodSymbol] = {
    val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
    if (constructorSymbol.isMethod) Some(constructorSymbol.asMethod)
    else {
      val ctors = constructorSymbol.asTerm.alternatives
      ctors.map(_.asMethod).find(_.isPrimaryConstructor)
    }
  }

  def fieldsOf(tpe: c.universe.Type): List[(c.universe.Symbol, c.universe.Type)] = {
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

  /**
    * Returns true if the given type is a value class.
    */
  def isValueClass(tpe: Type): Boolean = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isDerivedValueClass // todo why was this here: && fixedAnnotation.isEmpty

  /**
    * Returns true if the given type is a class or trait and is sealed.
    */
  def isSealed(tpe: Type): Boolean = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isSealed

  /**
    * If a value class, returns the type that the value class is wrapping.
    * Otherwise returns the type itself
    */
  def underlyingType(tpe: Type): c.universe.Type = if (isValueClass(tpe)) {
    tpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head.typeSignature
  } else {
    tpe
  }

  /**
    * Returns the method that generates the default value for the field identified by the offset.
    * Note: the index offset should be + 1 since 0 is reserved for something which I forget.
    * Nominally, the method is named `name$default$N` where in the case of constructor defaults,
    * name would be <init>.
    *
    * If the field has no default value this method will error, so ensure you only
    * call it if you are sure the field has a default value.
    */
  def defaultGetter(tpe: Type, index: Int): c.universe.MethodSymbol = {
    val getter = defswithsymbols.nme.defaultGetterName(defswithsymbols.nme.CONSTRUCTOR, index + 1)
    // this is a method symbol for the default getter if it exists
    tpe.companion.member(TermName(getter.toString)).asMethod
  }

  /**
    * Returns all the annotations of the given symbol by encoding them
    * as instances of [Anno].
    */
  def annotationsqq(sym: Symbol): List[c.universe.Tree] = sym.annotations.map { a =>
    val name = a.tree.tpe.typeSymbol.fullName
    val args = a.tree.children.tail.map(_.toString.stripPrefix("\"").stripSuffix("\""))
    q"_root_.com.sksamuel.avro4s.internal.Anno($name, $args)"
  }

  def annotations(sym: Symbol): List[Anno] = sym.annotations.map { a =>
    val name = a.tree.tpe.typeSymbol.fullName
    val args = a.tree.children.tail.map(_.toString.stripPrefix("\"").stripSuffix("\""))
    com.sksamuel.avro4s.internal.Anno(name, args)
  }
}

object ReflectHelper {
  def apply[C <: whitebox.Context](c: C): ReflectHelper[c.type] = new ReflectHelper[c.type](c)
}
