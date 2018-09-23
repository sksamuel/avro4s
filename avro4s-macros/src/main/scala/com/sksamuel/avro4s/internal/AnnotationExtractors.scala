package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.{AvroAlias, AvroDoc, AvroFixed, AvroName, AvroNamespace, AvroProp}

class AnnotationExtractors(annos: Seq[Anno]) {

  // returns the value of the first arg from the first annotation that matches the given class
  private def findFirst(clazz: Class[_]): Option[String] = annos.find(_.className == clazz.getName).map(_.args.head.toString)

  private def findAll(clazz: Class[_]): Seq[String] = annos.filter(_.className == clazz.getName).map(_.args.head.toString)

  def namespace: Option[String] = findFirst(classOf[AvroNamespace])
  def doc: Option[String] = findFirst(classOf[AvroDoc])
  def aliases: Seq[String] = findAll(classOf[AvroAlias])

  def fixed: Option[Int] = findFirst(classOf[AvroFixed]).map(_.toInt)

  def name: Option[String] = findFirst(classOf[AvroName])

  def props: Map[String, String] = annos.filter(_.className == classOf[AvroProp].getName).map { anno =>
    anno.args.head.toString -> anno.args(1).toString
  }.toMap
}
