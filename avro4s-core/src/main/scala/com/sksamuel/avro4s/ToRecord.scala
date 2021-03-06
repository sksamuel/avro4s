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
  def apply[T](schema: Schema)(using encoder: Encoder[T]): ToRecord[T] = new ToRecord[T] {
    def to(t: T): Record = encoder.encode(schema).apply(t) match {
      case record: Record => record
      case output => {
        val clazz = output.getClass
        throw new Avro4sEncodingException(s"Cannot marshall an instance of $t to a Record (had class $clazz, output was $output)")
      }
    }
  }
}
