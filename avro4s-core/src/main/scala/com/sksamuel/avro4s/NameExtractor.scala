package com.sksamuel.avro4s

import magnolia.{Subtype, TypeName}

/**
  * Extracts name and namespace from a TypeName.
  * Takes into consideration provided annotations.
  */
case class NameExtractor(typeName: TypeName, nameAnnotation: Option[String], namespaceAnnotation: Option[String], erased: Boolean) {

  private val defaultNamespace = typeName.owner.replaceAll("\\.<local .*?>", "").stripSuffix(".package")

  // the name of the scala class without type parameters.
  // Eg, List[Int] would be List.
  private val erasedName = typeName.short

  // the name of the scala class with type parameters encoded,
  // Eg, List[Int] would be `List__Int`
  // Eg, Type[A, B] would be `Type__A_B`
  private val genericName = {
    if (typeName.typeArguments.isEmpty) {
      erasedName
    } else {
      val targs = typeName.typeArguments.map(_.short).mkString("_")
      typeName.short + "__" + targs
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
  def namespace: String = namespaceAnnotation.getOrElse(defaultNamespace)

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
  def name: String = nameAnnotation.getOrElse {
    if (erased) erasedName else genericName
  }
}

object NameExtractor {
  def apply[F[_], T](subtype: Subtype[F, T]): NameExtractor = NameExtractor(subtype.typeName, subtype.annotations)

  def apply(typeName: TypeName, annos: Seq[Any]): NameExtractor = {
    val extractor = new AnnotationExtractors(annos)
    NameExtractor(typeName, extractor.name, extractor.namespace, extractor.erased)
  }

  def apply[A](clazz: Class[A]): NameExtractor = {

    import scala.reflect.runtime.universe

    val mirror = universe.runtimeMirror(clazz.getClassLoader)
    val sym = mirror.classSymbol(clazz)
    val tpe = sym.toType

    def tpe2name(tpe: universe.Type): TypeName = {
      TypeName(tpe.typeSymbol.owner.fullName, tpe.typeSymbol.name.decodedName.toString, tpe.typeArgs.map(tpe2name))
    }

    import scala.reflect.runtime.universe._

    val nameAnnotation = sym.typeSignature.typeSymbol.annotations.collectFirst {
      case a if a.tree.tpe =:= typeOf[AvroName] =>
        val annoValue = a.tree.children.tail.head.asInstanceOf[Literal].value.value
        annoValue.toString
    }

    val namespaceAnnnotation = sym.typeSignature.typeSymbol.annotations.collectFirst {
      case a if a.tree.tpe =:= typeOf[AvroNamespace] =>
        val annoValue = a.tree.children.tail.head.asInstanceOf[Literal].value.value
        annoValue.toString
    }

    val erased = sym.typeSignature.typeSymbol.annotations.exists {
      case a if a.tree.tpe =:= typeOf[AvroErasedName] => true
      case _ => false
    }

    val typeName = tpe2name(tpe)
    NameExtractor(typeName, nameAnnotation, namespaceAnnnotation, erased)
  }
}
