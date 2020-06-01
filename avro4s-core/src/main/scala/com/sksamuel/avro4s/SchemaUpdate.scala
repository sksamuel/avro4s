package com.sksamuel.avro4s

sealed trait SchemaUpdate

/**
 * This ADT is used to capture Avro schema updates that need to be passed from one type to another. There are two
 * kinds of schema updates:
 *
 * a) a full schema replacement that is triggered by e.g. `.withSchema` on [[Encoder]] and [[Decoder]], or by
 * [[AvroFixed]] annotations on a field or a value class. This is represented
 * by [[com.sksamuel.avro4s.SchemaUpdate.FullSchemaUpdate]]
 *
 * b) a namespace change given via [[AvroNamespace]] annotation on a sealed trait to its implementing case classes, or
 *    given on a field that is passed to the referenced type.
 *
 *
 * @see [[ResolvableEncoder]], [[ResolvableDecoder]], [[ResolvableSchemaFor]], [[Encoder.withSchema]], [[Decoder.withSchema]]
 */
object SchemaUpdate {
  case class FullSchemaUpdate(schemaFor: SchemaFor[_]) extends SchemaUpdate
  case class NamespaceUpdate(namespace: String) extends SchemaUpdate
  case object NoUpdate extends SchemaUpdate
}
