package com.sksamuel.avro4s

import scala.reflect.runtime.universe

object AvroNamespaceResolver {
  def forClass(tpe: universe.Type): Any = {
    val fullName = tpe.typeSymbol.fullName
    val defaultNamespace = fullName.split('.').dropRight(1).mkString(".")

    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    new AnnotationExtractors(annos).namespace.getOrElse(defaultNamespace)
  }
}
