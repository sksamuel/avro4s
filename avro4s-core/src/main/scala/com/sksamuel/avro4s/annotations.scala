package com.sksamuel.avro4s

import scala.annotation.StaticAnnotation

case class AvroDoc(val doc: String) extends StaticAnnotation

case class AvroNamespace(val doc: String) extends StaticAnnotation