package com.sksamuel.avro4s

import magnolia.{Subtype, TypeName}

import scala.collection.JavaConverters._
import java.util

/**
  * Extracts name and namespace from a TypeName.
  * Takes into consideration provided annotations.
  */
case class NameExtractor(typeInfo: TypeInfo) {

  private val defaultNamespace = typeInfo.owner.replaceAll("\\.<local .*?>", "").stripSuffix(".package")

  // the name of the scala class without type parameters.
  // Eg, List[Int] would be List.
  private val erasedName = typeInfo.short

  // the name of the scala class with type parameters encoded,
  // Eg, List[Int] would be `List__Int`
  // Eg, Type[A, B] would be `Type__A_B`
  // this method must also take into account @AvroName on the classes used as type arguments
  private val genericName = {
    if (typeInfo.typeArguments.isEmpty) {
      erasedName
    } else {
      val targs = typeInfo.typeArguments.map { typeArgInfo => NameExtractor(typeArgInfo).name }.mkString("_")
      typeInfo.short + "__" + targs
    }
  }

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
  def namespace: String = typeInfo.namespaceAnnotation.getOrElse(defaultNamespace)

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
  def name: String = typeInfo.nameAnnotation.getOrElse {
    if (typeInfo.erased) erasedName else genericName
  }
}

object NameExtractor {
  def apply[F[_], T](subtype: Subtype[F, T]): NameExtractor = NameExtractor(subtype.typeName, subtype.annotations)

  // caching the name extractor instances, as it is expensive to build them. As of writing, the performance improvement
  // on the encoding / decoding benchmarks show that this yields a roughly x 3 speedup.
  // see https://github.com/sksamuel/avro4s/issues/422#issuecomment-570770700 for more details.
  private val typeNameCache: scala.collection.concurrent.Map[(TypeName, Seq[Any]), NameExtractor] =
    new util.concurrent.ConcurrentHashMap[(TypeName, Seq[Any]), NameExtractor]().asScala

  def apply(typeName: TypeName, annos: Seq[Any]): NameExtractor =
    typeNameCache.getOrElseUpdate((typeName, annos), NameExtractor(TypeInfo(typeName, annos)))

  def apply[A](clazz: Class[A]): NameExtractor = NameExtractor(TypeInfo.fromClass(clazz))
}
