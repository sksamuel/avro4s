package com.sksamuel.avro4s

import scala.reflect.runtime.universe

object AvroNameResolver {
  def forClass(tpe: universe.Type): Any = {
    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    val className = tpe.typeSymbol.name.decodedName
    new AnnotationExtractors(annos).name.getOrElse(className)
  }
}