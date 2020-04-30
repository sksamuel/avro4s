package com.sksamuel.avro4s

/**
 * Indicates that the Encoder/Decoder/Codec supports namespaces - this is the case for all records-related operations.
 */
trait NamespaceAware[T] {

  /**
   * Create a new transformer with the namespace being as provided
   */
  def withNamespace(namespace: String): T

}
