package com.sksamuel.avro4s

import scala.reflect.macros.whitebox

object AvroNameResolver {

  import scala.reflect.runtime.universe

  /**
    * Returns the namespace for this type to be used when creating
    * an avro record. This method takes into account @AvroNamespace.
    */
  def namespace(tpe: universe.Type): String = {
    val packageName = ReflectHelper.packageName(tpe.typeSymbol)
    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    new AnnotationExtractors(annos).namespace.getOrElse(packageName)
  }

  // **** IF someone knows how to unify the types from the macro context and the reflect mirror
  // so I don't need to duplicate these methods I will buy you a pint! *****
  def namespace[C <: whitebox.Context](c: C)(tpe: c.Type): String = {
    val reflect = ReflectHelper(c)
    val packageName = reflect.packageName(tpe)
    val annos = reflect.annotations(tpe.typeSymbol)
    new AnnotationExtractors(annos).namespace.getOrElse(packageName)
  }

  /**
    * Returns the record name for this type to be used when creating
    * an avro record. This method takes into account type parameters and
    * annotations.
    *
    * The general format for a record name is `resolved-name__typea_typeb_typec`.
    * That is a double underscore delimits the resolved name from the start of the
    * type parameters and then each type parameter is delimited by a single underscore.
    *
    * The resolved name is the class name with any annotations applied, such
    * as @AvroName or @AvroNamespace, or @AvroErasedName, which, if present,
    * means the type parameters will not be included in the final name.
    */
  def name(tpe: universe.Type): String = {
    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    val extractor = new AnnotationExtractors(annos)
    val typeName = tpe.typeSymbol.name.decodedName.toString
    val baseName = extractor.name.getOrElse(typeName)
    if (extractor.erased) {
      baseName
    } else {
      tpe.typeArgs match {
        case Nil => baseName
        case args => baseName + "__" + args.map(name).mkString("_")
      }
    }
  }

  // **** IF someone knows how to unify the types from the macro context and the reflect mirror
  // so I don't need to duplicate these methods I will buy you a pint! *****
  def name[C <: whitebox.Context](c: C)(tpe: c.Type): String = {
    val reflect = ReflectHelper(c)
    val annos = reflect.annotations(tpe.typeSymbol)
    val extractor = new AnnotationExtractors(annos)
    val typeName = tpe.typeSymbol.name.decodedName.toString
    val baseName = extractor.name.getOrElse(typeName)
    if (extractor.erased) {
      baseName
    } else {
      tpe.typeArgs match {
        case Nil => baseName
        case args => baseName + "__" + args.map(name(c)(_)).mkString("_")
      }
    }
  }

  /**
    * Returns the full record name (namespace + name) for use in an Avro
    * record taking into account annotations and type parameters.
    */
  def fullName(tpe: universe.Type): String = namespace(tpe) + "." + name(tpe)

  // **** IF someone knows how to unify the types from the macro context and the reflect mirror
  // so I don't need to duplicate these methods I will buy you a pint! *****
  def fullName[C <: whitebox.Context](c: C)(tpe: c.Type) = namespace(c)(tpe) + "." + name(c)(tpe)

  @deprecated("use the nicer methods in this class", "2.0.2")
  def forClass(tpe: universe.Type): Any = {
    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    val className = tpe.typeSymbol.name.decodedName
    new AnnotationExtractors(annos).name.getOrElse(className)
  }
}