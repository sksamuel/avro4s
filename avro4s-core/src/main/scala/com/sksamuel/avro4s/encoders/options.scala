package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder, FieldMapper}
import org.apache.avro.Schema

class OptionEncoder[T](encoder: Encoder[T]) extends Encoder[Option[T]] {

  override def encode(option: Option[T], schema: Schema): Any = {
    // nullables must be encoded with a union of 2 elements, where null is the first type
    require(schema.getType == Schema.Type.UNION, {
      "Options can only be encoded with a UNION schema"
    })
    require(schema.getTypes.size() == 2, {
      "Options can only be encoded with a 2 element union schema"
    })
    require(schema.getTypes.get(0).getType == Schema.Type.NULL)
    val elementSchema = schema.getTypes.get(1)
    option.fold(null)(value => encoder.encode(value, elementSchema))
  }
}

trait OptionEncoders:
  given[T](using encoder: Encoder[T]): Encoder[Option[T]] = OptionEncoder[T](encoder)