package com.sksamuel.avro4s.typeutils

import magnolia1.TypeInfo

/**
  * Extracts names and namespaces from a type.
  * Takes into consideration provided annotations.
  */
case class Names(typeInfo: TypeInfo, val annos: Annotations) {

  private val defaultNamespace = typeInfo.owner.replaceAll("\\.<local .*?>", "").stripSuffix(".package")

  // the name of the scala class without type parameters.
  // Eg, List[Int] would be List.
  private val erasedName = typeInfo.short

  // the name of the scala class with type parameters encoded,
  // Eg, List[Int] would be `List__Int`
  // Eg, Type[A, B] would be `Type__A_B`
  // this method must also take into account @AvroName on the classes used as type arguments
  private val genericName = {
    if (typeInfo.typeParams.isEmpty) {
      erasedName
    } else {
      val targs = typeInfo.typeParams.map { tparam => Names(tparam).name }.mkString("_")
      typeInfo.short + "__" + targs
    }
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
    * The name is the class name with any annotations applied, such
    * as @AvroName or @AvroNamespace, or @AvroErasedName, which, if present,
    * means the type parameters will not be included in the final name.
    */
  def name: String = annos.name.getOrElse {
    if (annos.erased) erasedName else genericName
  }

  /**
    * Returns the namespace for this type to be used when creating
    * an avro record. This method takes into account @AvroNamespace.
    */
  def namespace: String = annos.namespace.getOrElse(defaultNamespace)

  /**
    * Returns the full record name (namespace + name) for use in an Avro
    * record taking into account annotations and type parameters.
    */
  def fullName: String = namespace.trim() match {
    case "" => name
    case otherwise => namespace + "." + name
  }

}

object Names {
  def apply(info: TypeInfo): Names = Names(info, Annotations(Nil))
  //  def apply[F[_], T](subtype: Subtype[F, T]): NameExtractor = NameExtractor(subtype.typeName, subtype.annotations)
  //
  //  def apply(typeName: TypeName, annos: Seq[Any]): NameExtractor = NameExtractor(TypeInfo(typeName, annos))
  //
  //  def apply[A](clazz: Class[A]): NameExtractor = NameExtractor(TypeInfo.fromClass(clazz))
}
