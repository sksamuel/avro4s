package com.sksamuel.avro4s

import org.apache.avro.Schema

/**
  * Converts from instances of T into Record's.
  *
  * Note: This interface requires that T is marshalled
  * to an Avro GenericRecord / SpecificRecord, and therefore
  * is limited to use by case classes or traits. This interface
  * is essentially just a convenience wrapper around an
  * Encoder so you do not have to cast the result.
  *
  * If you wish to convert an avro type other than record use an Encoder directly.
  *
  */
trait ToRecord[T] extends Serializable {
  def to(t: T): Record
}

object ToRecord {
  def apply[T: Encoder : SchemaFor]: ToRecord[T] = apply(AvroSchema[T])
  def apply[T](schema: Schema)(implicit encoder: Encoder[T], fieldMapper: FieldMapper = DefaultFieldMapper): ToRecord[T] = new ToRecord[T] {
    override def to(t: T): Record = encoder.encode(t, schema, fieldMapper) match {
      case record: Record => record
      case output => sys.error(s"Cannot marshall an instance of $t to a Record (was $output)")
    }
  }
}