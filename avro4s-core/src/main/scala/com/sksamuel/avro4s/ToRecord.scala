package com.sksamuel.avro4s

import org.apache.avro.Schema

/**
  * Converts from instances of T into [[Record]]s.
  *
  * Note: This interface requires that T is marshalled
  * to an Avro GenericRecord / SpecificRecord, and therefore
  * is limited to use by case classes. This interface
  * is essentially just a conveniece wrapper around an
  * [[Encoder]] so you do not have to cast the result.
  *
  * If you wish to convert a primitive or top level type, such
  * as an Array, then use an [[Encoder]] directly.
  *
  */
trait ToRecord[T <: Product] extends Serializable {
  def to(t: T): Record
}

object ToRecord {
  def apply[T <: Product : Encoder : SchemaFor]: ToRecord[T] = apply(AvroSchema[T])
  def apply[T <: Product](schema: Schema)(implicit encoder: Encoder[T]): ToRecord[T] = new ToRecord[T] {
    override def to(t: T): Record = encoder.encode(t, schema) match {
      case record: Record => record
      case output => sys.error(s"Cannot marshall an instance of $t to a Record (was $output)")
    }
  }
}