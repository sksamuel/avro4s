package com.sksamuel.avro4s

import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.macros.whitebox
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.util.{Try => ScalaTry}

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

  def constructorParameters(tpe: c.universe.Type): List[(c.universe.Symbol, c.universe.Type)] = {
    primaryConstructor(tpe).map { constructor =>
      val constructorTypeContext = constructor.typeSignatureIn(tpe).dealias
      constructorTypeContext.paramLists.headOption.map { symbols =>
        symbols.map(s => s -> s.typeSignatureIn(constructorTypeContext).dealias)
      }.getOrElse(Nil)
    }.getOrElse(Nil)
  }

  /**
    * For the given type, returns backing fields of the case class.
    */
  def caseClassFields(tpe: c.universe.Type): List[(c.universe.Symbol, c.universe.Type)] = {
    tpe.decls.collect {
      case t: TermSymbol if t.isCaseAccessor && t.isVal => t -> t.typeSignatureIn(tpe)
    }.toList
  }

  /**
    * Returns true if the given type has a field of the given name marked as @transient.
    */
  def isTransientOnField(tpe: c.universe.Type, sym: c.universe.Symbol): Boolean = {
    tpe.decls
      .filter(_.isTerm)
      .map(_.asTerm)
      .filter(f => f.isVal && f.getter.isMethod && f.getter.name == sym.name)
      .exists(isTransient)
  }

  /**
    * Returns true if this symbol contains an @transient annotation.
    */
  def isTransient(sym: c.universe.Symbol): Boolean = {
    sym.annotations.exists { a =>
      a.tree.tpe <:< typeOf[transient]
    }
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

  def isLibraryType(tpe: Type): Boolean = tpe.typeSymbol.fullName.startsWith("scala") || tpe.typeSymbol.fullName.startsWith("java") || tpe.typeSymbol.fullName.startsWith("javax")

  def isShapelessType(tpe: Type): Boolean = tpe.typeSymbol.fullName.contains("shapeless")

  /**
    * Returns true if SchemaFor, Encoder or Decoder for this type should be generated
    * by an avro4s or shapeless macro. Returns false if there should be an implicit in scope
    * without using a macro.
    */
  def isMacroGenerated(tpe: Type): Boolean = {
    !isSealed(tpe) && !isShapelessType(tpe) && !isLibraryType(tpe) && !isScalaEnum(tpe) && isCaseClass(tpe)
  }

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
    Iterator.iterate(tpe.typeSymbol.owner)(_.owner)
      .dropWhile(x => !x.isPackage && !x.isModuleClass && !x.isModule)
      .dropWhile(_.name.decodedName.toString == "package")
      .take(1)
      .toList
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
    */
  def defaultGetter(tpe: Type, index: Int): c.universe.MethodSymbol = {

    // Internal: the index offset will be + 1 since 0 is reserved for something which I forget.
    // Nominally, the method is named name$default$N where in the case of constructor defaults,
    // name would be <init>.

    val getter = defswithsymbols.nme.defaultGetterName(defswithsymbols.nme.CONSTRUCTOR, index + 1)
    // this is a method symbol for the default getter if it exists
    tpe.companion.member(TermName(getter.toString)).asMethod
  }

  /**
    * Returns all the annotations of the given symbol by encoding them
    * as instances of [Anno].
    */
  def annotationsqq(sym: Symbol): List[c.universe.Tree] =
    annotationsHelper(sym).map {
      case (name, args) =>
      q"_root_.com.sksamuel.avro4s.Anno($name, $args)"
    }

  def annotations(sym: Symbol): List[Anno] =
    annotationsHelper(sym).map(Anno.tupled)

  //extract relevant annotation (name, args) tuples
  private def annotationsHelper(sym: Symbol): List[(String, Map[String,String])] = sym.annotations.map { a =>
    val name = a.tree.tpe.typeSymbol.fullName
    val tb = currentMirror.mkToolBox()

    val args = ScalaTry(tb.compile(tb.parse(a.toString)).apply())
      .toOption
      .collect {
        case c: AvroFieldReflection => c.getAllFields.map{case (k,v) => (k,v.toString)}
      }
      .getOrElse(Map.empty[String, String])
    (name, args)
  }


  def defaultNamespace(sym: c.universe.Symbol): String = sym.fullName.split('.').dropRight(1).mkString(".")
}

object ReflectHelper {

  def apply[C <: whitebox.Context](c: C): ReflectHelper[c.type] = new ReflectHelper[c.type](c)

  import scala.reflect.runtime.universe._

  // this impl must be kept inline with the impl in the reflect instance class
  def packageName(sym: Symbol) = Iterator.iterate(sym.owner)(_.owner)
    .dropWhile(x => !x.isPackage && !x.isModuleClass && !x.isModule)
    .dropWhile(_.name.decodedName.toString == "package")
    .take(1)
    .toList
    .head
    .fullName

  def annotations(sym: Symbol): List[Anno] =
    annotationsHelper(sym).map(Anno.tupled)

  //extract relevant annotation (name, args) tuples
  private def annotationsHelper(sym: Symbol): List[(String, Map[String,String])] = sym.annotations.map { a =>
    val name = a.tree.tpe.typeSymbol.fullName
    val tb = currentMirror.mkToolBox()

    val args = ScalaTry(tb.compile(tb.parse(a.toString)).apply())
      .toOption
      .collect {
        case c: AvroFieldReflection => c.getAllFields.map{case (k,v) => (k,v.toString)}
      }
      .getOrElse(Map.empty[String, String])
    (name, args)
  }

  def defaultNamespace(sym: universe.Symbol): String = sym.fullName.split('.').dropRight(1).mkString(".")
}
