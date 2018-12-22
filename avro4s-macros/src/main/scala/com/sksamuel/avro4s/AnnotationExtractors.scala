package com.sksamuel.avro4s

import org.codehaus.jackson.map.ObjectMapper
import collection.JavaConverters._

case class Anno(className: String, args: Map[String,String])

class AnnotationExtractors(annos: Seq[Anno]) {

  // returns the value of the first arg from the first annotation that matches the given class
  private def findFirst(argument: String, clazz: Class[_]): Option[String] = annos.find(c => clazz.isAssignableFrom(Class.forName(c.className))).flatMap(_.args.get(argument).map(_.toString))

  private def findAll(argument: String, clazz: Class[_]): Seq[String] = annos.filter(c => clazz.isAssignableFrom(Class.forName(c.className))).flatMap(_.args.get(argument).map(_.toString))

  private def exists(clazz: Class[_]): Boolean = annos.exists(c => clazz.isAssignableFrom(Class.forName(c.className)))

  def namespace: Option[String] = findFirst("namespace", classOf[AvroNamespaceable])
  def doc: Option[String] = findFirst("doc", classOf[AvroDocumentable])
  def aliases: Seq[String] = findAll("alias",classOf[AvroAliasable])

  def fixed: Option[Int] = findFirst("size",classOf[AvroFixable]).map(_.toInt)

  def name: Option[String] = findFirst("name",classOf[AvroNameable])

  def props: Map[String, String] = annos.filter(c => classOf[AvroProperty].isAssignableFrom(Class.forName(c.className)))
    .flatMap(x => {
      val mapper = new ObjectMapper()
      //Needs to bec converted into a java.lang.Object due to the fact that jackson serializes data like "true" as a boolean rather then a stringified boolean
      mapper.readValue(x.args("properties").getBytes, classOf[java.util.Map[String,java.lang.Object]]).asScala.map{case (k,v) => (k,v.toString)}
    }).toMap

  def erased: Boolean = exists(classOf[AvroErasedName])
}