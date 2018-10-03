package com.sksamuel.avro4s

import scala.reflect.runtime.universe

object AvroNameHelper {
  def forClass(tpe: universe.Type): Any = {
    val annos = ReflectHelper.annotations(tpe.typeSymbol)
    // we use the name of class if no @AvroName is present on the type
    new AnnotationExtractors(annos).name.getOrElse(tpe.typeSymbol.name.decodedName)
  }
}
