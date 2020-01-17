package com.sksamuel.avro4s

trait ModifiableNamespaceCodec[T] { self: Codec[T] =>

  def withNamespace(namespace: String): Codec[T]

}
