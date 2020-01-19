package com.sksamuel.avro4s

// TODO maybe align type parameters with SchemaAware for consistency.
trait NamespaceAware[T] {

  def withNamespace(namespace: String): T

}
