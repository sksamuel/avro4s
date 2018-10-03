package com.sksamuel.avro4s

import scala.reflect.runtime.universe

object AvroNameResolver {
  def forClass(tpe: universe.Type): Any = {
    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    val className = tpe.typeSymbol.name.decodedName
    new AnnotationExtractors(annos).name.getOrElse(className)
  }
}

object AvroNamespaceResolver {
  def forClass(tpe: universe.Type): Any = {
    val packageName = ReflectHelper.packageName(tpe.typeSymbol)
    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    new AnnotationExtractors(annos).namespace.getOrElse(packageName)
  }
}