package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema

class OptionEncoder[T](encoder: Encoder[T]) extends Encoder[Option[T]] {
  
  override def encode(schema: Schema, mapper: FieldMapper): Option[T] => Any = {
    // nullables must be encoded with a union of 2 elements, where null is the first type
    require(schema.getType == Schema.Type.UNION, { "Options can only be encoded with a UNION schema" })
    require(schema.getTypes.size() == 2, { "Options can only be encoded with a 2 element union schema" })
    require(schema.getTypes.get(0).getType == Schema.Type.NULL)
    val elementSchema = schema.getTypes.get(1)
    println("ElementSchema=" + elementSchema)
    val enc = encoder.encode(elementSchema, mapper)
    { option => option.fold(null)(value => enc(value)) }
  }
}

trait OptionEncoders {
  given [T](using encoder: Encoder[T]): Encoder[Option[T]] = OptionEncoder[T](encoder)
}