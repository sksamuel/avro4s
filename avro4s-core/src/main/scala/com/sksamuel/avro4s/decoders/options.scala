package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Decoder, Encoder}
import org.apache.avro.Schema

class OptionDecoder[T](decoder: Decoder[T]) extends Decoder[Option[T]] {

  override def decode(schema: Schema): Any => Option[T] = {
    // nullables must be encoded with a union of 2 elements, where null is the first type
    require(schema.getType == Schema.Type.UNION, {
      "Options can only be encoded with a UNION schema"
    })
    require(schema.getTypes.size() == 2, {
      "Options can only be encoded with a 2 element union schema"
    })
    require(schema.getTypes.get(0).getType == Schema.Type.NULL, {
      "Options can only be encoded with a UNION schema with NULL as the first element type"
    })
    val elementSchema = schema.getTypes.get(1)
    val decode = decoder.decode(elementSchema)
    { value => if (value == null) None else Some(decode(value)) }
  }
}

trait OptionDecoders:
  given[T](using decoder: Decoder[T]): Decoder[Option[T]] = new OptionDecoder[T](decoder)