package com.sksamuel.avro4s

case class NameResolution(defaultName: String, defaultNamespace: String, annos: Seq[Anno]) {
  private val extractor = new AnnotationExtractors(annos)

  def fullName(): String = {
    namespace + "." + name
  }

  def name(): String = {
    extractor.name.getOrElse(defaultName)
  }

  def namespace(): String = {
    extractor.namespace.getOrElse(defaultNamespace)
  }

}
