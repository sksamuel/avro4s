package com.sksamuel.avro4s

case class Anno(className: String, args: Map[String,String])

class AnnotationExtractors(annos: Seq[Any]) {

  // returns the value of the first arg from the first annotation that matches the given class
  private def findFirst[T: Manifest]: Option[T] = annos.collectFirst {
    case t: Any if manifest.runtimeClass.isAssignableFrom(t.getClass) => t.asInstanceOf[T]
  }

  private def findAll[T: Manifest]: Seq[T] = annos.collect {
    case t: Any if manifest.runtimeClass.isAssignableFrom(t.getClass) => t.asInstanceOf[T]
  }

  private def exists[T: Manifest]: Boolean = annos.exists { a => manifest.runtimeClass.isAssignableFrom(a.getClass) }

  def namespace: Option[String] = findFirst[AvroNamespaceable].map(_.namespace)
  def doc: Option[String] = findFirst[AvroDocumentable].map(_.doc)
  def aliases: Seq[String] = findAll[AvroAliasable].map(_.alias).filterNot(_.trim.isEmpty)
  def fixed: Option[Int] = findFirst[AvroFixable].map(_.size)
  def name: Option[String] = findFirst[AvroNameable].map(_.name).filterNot(_.trim.isEmpty)
  def sortPriority: Option[Float] = findFirst[AvroSortPriority].map(_.priority)
  def defaultValue: Option[String] = findFirst[AvroDefault].map(_.value)

  def enumDefault: Option[Any] = findFirst[AvroEnumDefault].map(_.default)

  def props: Map[String, String] = findAll[AvroProperty].map { prop =>
    prop.key -> prop.value
  }.toMap

  def nodefault: Boolean = exists[AvroNoDefault]
  def transient: Boolean = exists[AvroTransient]
  def erased: Boolean = exists[AvroErasedName]
}
