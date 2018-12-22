package com.sksamuel.avro4s

import scala.util.matching.Regex

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

  def props: Map[String, String] = annos.filter(c => classOf[AvroProperty].isAssignableFrom(Class.forName(c.className))).flatMap(x => stringToMap(x.args("properties"))).toMap

  private def stringToMap(string: String): Map[String,String] = {
    //Get the map elements
    AnnotationExtractors.betweenParentheses.findFirstIn(string)
      //Drop the surrounding ()
      .map(_.drop(1).dropRight(1)) match {
      case Some(mapElements) =>
        //Get list of tuples (ie a -> b)
        mapElements.split(", ").toList
          //Break make into tuples and convert into map
        .flatMap(_.split(AnnotationExtractors.arrowsNotInQuotes)).grouped(2).collect { case a :: b :: Nil => (a,b) }.toMap
      case None => Map.empty
    }
  }


  def erased: Boolean = exists(classOf[AvroErasedName])
}

object AnnotationExtractors {
  private val betweenParentheses: Regex = "\\(([^\\)]+)\\)".r
  private val arrowsNotInQuotes: String = "\\s(?!\\B\"[^\"]*)->(?![^\"]*\"\\B)\\s"
}
