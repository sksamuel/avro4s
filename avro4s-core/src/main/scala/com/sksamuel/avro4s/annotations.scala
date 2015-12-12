package com.sksamuel.avro4s

import scala.annotation.StaticAnnotation

case class AvroDoc(doc: String) extends StaticAnnotation

case class AvroFixed(size: Int) extends StaticAnnotation

