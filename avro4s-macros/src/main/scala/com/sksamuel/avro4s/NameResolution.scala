package com.sksamuel.avro4s

import magnolia.TypeName

import scala.reflect.macros.whitebox

/**
  * Implements methods to retrieve a suitable record name and namespace
  * for a given Scala type, taking into account type parameters and annotations.
  */
case class Namer(typeName: TypeName, annos: Seq[Any]) {

  private val extractor = new AnnotationExtractors(annos)

  private val defaultNamespace = typeName.owner.replaceAll("\\.<local .*?>", "").stripSuffix(".package")

  // the name of the scala class without type parameters.
  // Eg, List[Int] would be List.
  private val erasedName = typeName.short

  // the name of the scala class with type parameters encoded,
  // Eg, List[Int] would be `List__Int`
  private val genericName = typeName.short

  /**
    * Returns the full record name (namespace + name) for use in an Avro
    * record taking into account annotations and type parameters.
    */
  def fullName: String = namespace.trim() match {
    case "" => name
    case otherwise => namespace + "." + name
  }

  /**
    * Returns the namespace for this type to be used when creating
    * an avro record. This method takes into account @AvroNamespace.
    */
  def namespace: String = extractor.namespace.getOrElse(defaultNamespace)

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
  def name: String = {
    if (extractor.erased) {
      extractor.name.getOrElse(erasedName)
    } else {
      extractor.name.getOrElse(genericName)
    }
  }
}

/**
  * Implements methods to retrieve a suitable record name and namespace
  * for a given Scala type, taking into account type parameters and annotations.
  *
  * @param erasedName       the name of the scala class without type parameters.
  *                         Eg, List[Int] would be List.
  * @param genericName      the name of the scala class with type parameters encoded,
  *                         Eg, List[Int] would be `List__Int`
  * @param defaultNamespace usually the package name, or if the class is nested, then the package name + outer class
  */
case class NameResolution(erasedName: String,
                          genericName: String,
                          defaultNamespace: String,
                          annos: Seq[Anno]) {

  private val extractor = new AnnotationExtractors(annos)

  /**
    * Returns the full record name (namespace + name) for use in an Avro
    * record taking into account annotations and type parameters.
    */
  def fullName: String = namespace.trim() match {
    case "" => name
    case otherwise => namespace + "." + name
  }

  /**
    * Returns the namespace for this type to be used when creating
    * an avro record. This method takes into account @AvroNamespace.
    */
  def namespace: String = extractor.namespace.getOrElse(defaultNamespace)

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
  def name: String = {
    if (extractor.erased) {
      extractor.name.getOrElse(erasedName)
    } else {
      extractor.name.getOrElse(genericName)
    }
  }
}

object NameResolution {

  import scala.reflect.runtime.universe

  def apply[C <: whitebox.Context](c: C)(tpe: c.Type): NameResolution = NameResolution(
    tpe.typeSymbol.name.decodedName.toString,
    GenericNameEncoder(c)(tpe),
    ReflectHelper(c).defaultNamespace(tpe.typeSymbol),
    ReflectHelper(c).annotations(tpe.typeSymbol)
  )

  def apply(tpe: universe.Type): NameResolution = NameResolution(
    tpe.typeSymbol.name.decodedName.toString,
    GenericNameEncoder(tpe),
    ReflectHelper.defaultNamespace(tpe.typeSymbol),
    ReflectHelper.annotations(tpe.typeSymbol)
  )

  def apply[A](clazz: Class[A]): NameResolution = {
    val mirror = universe.runtimeMirror(clazz.getClassLoader)
    val tpe = mirror.classSymbol(clazz).toType
    NameResolution(tpe)
  }
}
