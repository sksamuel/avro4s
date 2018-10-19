package com.sksamuel.avro4s

case class Anno(className: String, args: Seq[Any])

class AnnotationExtractors(annos: Seq[Anno]) {

  // returns the value of the first arg from the first annotation that matches the given class
  private def findFirst(clazz: Class[_]): Option[String] = annos.find(c => clazz.isAssignableFrom(Class.forName(c.className))).map(_.args.head.toString)

  private def findAll(clazz: Class[_]): Seq[String] = annos.filter(c => clazz.isAssignableFrom(Class.forName(c.className))).map(_.args.head.toString)

  private def exists(clazz: Class[_]): Boolean = annos.exists(c => clazz.isAssignableFrom(Class.forName(c.className)))

  def namespace: Option[String] = findFirst(classOf[AvroNamespaceable])
  def doc: Option[String] = findFirst(classOf[AvroDocumentable])
  def aliases: Seq[String] = findAll(classOf[AvroAliasable])

  def fixed: Option[Int] = findFirst(classOf[AvroFixable]).map(_.toInt)

  def name: Option[String] = findFirst(classOf[AvroNameable])

  def props: Map[String, String] = annos.filter( c=> classOf[AvroProperty].isAssignableFrom(Class.forName(c.className))).map { anno =>
    anno.args.head.toString -> anno.args(1).toString
  }.toMap

  def erased: Boolean = exists(classOf[AvroErasedName])
}
