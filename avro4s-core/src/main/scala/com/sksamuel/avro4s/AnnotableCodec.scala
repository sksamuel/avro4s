package com.sksamuel.avro4s

trait AnnotableCodec[T] { self: Codec[T] =>

  def withAnnotations(annotations: Seq[Any]): Codec[T]

}
