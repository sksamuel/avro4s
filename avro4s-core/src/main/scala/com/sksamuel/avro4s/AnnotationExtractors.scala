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

  def namespace: Option[String] = findFirst[AvroNamespaceable].map(_.namespace).filterNot(_.trim.isEmpty)
  def doc: Option[String] = findFirst[AvroDocumentable].map(_.doc)
  def aliases: Seq[String] = findAll[AvroAliasable].map(_.alias).filterNot(_.trim.isEmpty)
  def fixed: Option[Int] = findFirst[AvroFixable].map(_.size)
  def name: Option[String] = findFirst[AvroNameable].map(_.name).filterNot(_.trim.isEmpty)

  def props: Map[String, String] = findAll[AvroProperty].map { prop =>
    prop.key -> prop.value
  }.toMap

  def erased: Boolean = exists[AvroErasedName]
}

class AnnotationExtractors2(annos: Seq[Anno]) {

  // returns the value of the first arg from the first annotation that matches the given class
  private def findFirst(argument: String, clazz: Class[_]): Option[String] = annos.find(c => clazz.isAssignableFrom(Class.forName(c.className))).flatMap(_.args.get(argument).map(_.toString))

  private def findAll(argument: String, clazz: Class[_]): Seq[String] = annos.filter(c => clazz.isAssignableFrom(Class.forName(c.className))).flatMap(_.args.get(argument).map(_.toString))

  private def exists(clazz: Class[_]): Boolean = annos.exists(c => clazz.isAssignableFrom(Class.forName(c.className)))

  def namespace: Option[String] = findFirst("namespace", classOf[AvroNamespaceable])
  def doc: Option[String] = findFirst("doc", classOf[AvroDocumentable])
  def aliases: Seq[String] = findAll("alias",classOf[AvroAliasable])

  def fixed: Option[Int] = findFirst("size",classOf[AvroFixable]).map(_.toInt)

  def name: Option[String] = findFirst("name",classOf[AvroNameable])

  def props: Map[String, String] = annos.filter(c => classOf[AvroProperty].isAssignableFrom(Class.forName(c.className))).map { anno =>
    anno.args("key").toString -> anno.args("value").toString
  }.toMap

  def erased: Boolean = exists(classOf[AvroErasedName])
}