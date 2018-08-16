package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.{AvroDoc, AvroNamespace}

class AnnotationExtractors(annos: Seq[Anno]) {

  private def findFirst(clazz: Class[_]): Option[String] = annos.find(_.name == clazz.getName).map(_.args.head.toString)

  def namespace: Option[String] = findFirst(classOf[AvroNamespace])
  def doc: Option[String] = findFirst(classOf[AvroDoc])
}
