package com.sksamuel.avro4s.schema

import com.sksamuel.avro4s.AvroDoc
import com.sksamuel.avro4s.internal.Anno

object AnnotationExtractors {
  def doc(annos: Seq[Anno]): Option[String] = annos.find(_.name == classOf[AvroDoc].getName).map(_.args.head.toString)
}
