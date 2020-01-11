package com.sksamuel.avro4s

import org.apache.avro.Schema

/**
 * A [[SchemaFor]] generates an Avro Schema for a Scala or Java type.
 *
 * For example, a String SchemaFor could return an instance of Schema.Type.STRING
 * or Schema.Type.FIXED depending on the type required for Strings.
 */
trait SchemaForV2[T] extends Serializable {
  self =>

  def schema: Schema

  /**
   * Creates a SchemaFor[U] by applying a function Schema => Schema
   * to the schema generated by this instance.
   */
  def map[U](fn: Schema => Schema): SchemaForV2[U] = new SchemaForV2[U] {
    val schema: Schema = fn(self.schema)
  }
}

object SchemaForV2 {

  def apply[T](implicit schemaFor: SchemaForV2[T]): SchemaForV2[T] = schemaFor

  implicit def fromCodec[T](implicit codec: Codec[T]): SchemaForV2[T] = new Container[T](codec.schema)

  private class Container[T](val schema: Schema) extends SchemaForV2[T]
}


