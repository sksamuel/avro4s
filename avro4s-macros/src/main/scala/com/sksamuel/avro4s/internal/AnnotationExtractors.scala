package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.{AvroAlias, AvroDoc, AvroFixed, AvroName, AvroNamespace, AvroProp}

class AnnotationExtractors(annos: Seq[Anno]) {

  private def findFirst(clazz: Class[_]): Option[String] = annos.find(_.name == clazz.getName).map(_.args.head.toString)
  private def findAll(clazz: Class[_]): Seq[String] = annos.filter(_.name == clazz.getName).map(_.args.head.toString)

  def namespace: Option[String] = findFirst(classOf[AvroNamespace])
  def doc: Option[String] = findFirst(classOf[AvroDoc])
  def aliases: Seq[String] = findAll(classOf[AvroAlias])

  def fixed: Option[Int] = findFirst(classOf[AvroFixed]).map(_.toInt)

  def name: Option[String] = findFirst(classOf[AvroName])

  def props: Map[String, String] = annos.filter(_.name == classOf[AvroProp].getName).map { anno =>
    anno.args.head.toString -> anno.args(1).toString
  }.toMap
}
