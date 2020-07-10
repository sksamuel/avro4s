package com.sksamuel.avro4s

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
  def apply[T](implicit encoder: Encoder[T]): ToRecord[T] = new ToRecord[T] {

    val resolved = encoder.resolveEncoder()

    def to(t: T): Record = resolved.encode(t) match {
      case record: Record => record
      case output =>
        throw new Avro4sEncodingException(s"Cannot marshall an instance of $t to a Record (was $output)",
                                          output,
                                          encoder)
    }
  }
}
