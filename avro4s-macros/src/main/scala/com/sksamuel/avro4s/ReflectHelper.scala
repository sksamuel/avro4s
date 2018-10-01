package com.sksamuel.avro4s

import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.macros.whitebox

class ReflectHelper[C <: whitebox.Context](val c: C) {

  import c.universe
  import c.universe._

  private val defswithsymbols = universe.asInstanceOf[Definitions with SymbolTable with StdNames]

  def isScalaEnum(tpe: c.universe.Type): Boolean = tpe.<:<(typeOf[Enumeration])

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

  def isCaseClass(tpe: Type): Boolean = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass

  /**
    * Returns true if the given type is a value class.
    */
  def isValueClass(tpe: Type): Boolean = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isDerivedValueClass

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
    * Returns the package name for the given type, by iterating up the tree
    * until a package or a non-package object package is found.
    */
  def packageName(tpe: Type): String = {
    Stream.iterate(tpe.typeSymbol.owner)(_.owner)
      .dropWhile(_.name.decodedName.toString == "package")
      .dropWhile(x => !x.isPackage && !x.isModuleClass)
      .head
      .fullName
  }

  /**
    * Returns the method that generates the default value for the field identified by the offset.
    *
    * If the field has no default value this method will error, so ensure you only
    * call it if you are sure the field has a default value.
    *
    * Note: The companion must be defined for this to work.
    *
    * Internal: the index offset will be + 1 since 0 is reserved for something which I forget.
    * Nominally, the method is named name$default$N where in the case of constructor defaults,
    * name would be <init>.
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
    q"_root_.com.sksamuel.avro4s.Anno($name, $args)"
  }

  def annotations(sym: Symbol): List[Anno] = sym.annotations.map { a =>
    val name = a.tree.tpe.typeSymbol.fullName
    val args = a.tree.children.tail.map(_.toString.stripPrefix("\"").stripSuffix("\""))
    Anno(name, args)
  }

  /**
    * Returns the appropriate name for this type to be used when creating
    * an avro record. This method takes into account any type parameters
    * and whether the type has been annotated with @AvroErasedName.
    *
    * The format for a generated name is `rawname__typea_typeb_typec`. That is
    * a double underscore delimits the raw type from the start of the type
    * parameters and then each type parameter is delimited by a single underscore.
    */
  def recordName(tpe: Type): String = {
    val annos = annotations(tpe.typeSymbol)
    val erasedName = tpe.typeSymbol.name.decodedName.toString
    if (new AnnotationExtractors(annos).erased) {
      erasedName
    } else {
      tpe.typeArgs match {
        case Nil => erasedName
        case args => erasedName + "__" + args.map(recordName).mkString("_")
      }
    }
  }
}

object ReflectHelper {
  def apply[C <: whitebox.Context](c: C): ReflectHelper[c.type] = new ReflectHelper[c.type](c)
}
