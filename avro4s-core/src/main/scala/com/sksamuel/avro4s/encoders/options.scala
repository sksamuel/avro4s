package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Encoder, FieldMapper}
import org.apache.avro.Schema

class OptionEncoder[T](encoder: Encoder[T]) extends Encoder[Option[T]] {

  override def encode(schema: Schema): Option[T] => Any = {
    // nullables must be encoded with a union of 2 elements, where null is the first type
    require(schema.getType == Schema.Type.UNION, {
      "Options can only be encoded with a UNION schema"
    })
    require(schema.getTypes.size() >= 2, {
      "Options can only be encoded with a union schema with 2 or more types"
    })
    require(schema.getTypes.get(0).getType == Schema.Type.NULL)
    val schemaSize = schema.getTypes.size()
    val elementSchema = schemaSize match
      case 2 => schema.getTypes.get(1)
      case _ => Schema.createUnion(schema.getTypes.subList(1, schemaSize))
    val elementEncoder = encoder.encode(elementSchema)
    { option => option.fold(null)(value => elementEncoder(value)) }
  }
}

trait OptionEncoders:
  given[T](using encoder: Encoder[T]): Encoder[Option[T]] = OptionEncoder[T](encoder)