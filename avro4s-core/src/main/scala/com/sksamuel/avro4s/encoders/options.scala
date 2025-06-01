package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema

import scala.jdk.CollectionConverters.*

class OptionEncoder[T](encoder: Encoder[T]) extends Encoder[Option[T]] {

  override def encode(schema: Schema): Option[T] => AnyRef = {
    // nullables must be encoded with a union of 2 elements, one of which is null
    require(schema.getType == Schema.Type.UNION, {
      "Options can only be encoded with a UNION schema"
    })
    require(schema.getTypes.size() >= 2, {
      "Options can only be encoded with a union schema with 2 or more types"
    })
    val (isNull, isNotNull) = schema.getTypes().asScala.toList.partition(_.getType() == Schema.Type.NULL)
    require(isNull.size == 1, s"exactly one of the schemas must be null (found ${isNull.size})")

    val elementSchema = isNotNull match {
      case List(single) => single
      case more => Schema.createUnion(more.asJava)
    }
    val elementEncoder = encoder.encode(elementSchema)
    { option => option.fold(null)(elementEncoder) }
  }
}

trait OptionEncoders:
  given[T](using encoder: Encoder[T]): Encoder[Option[T]] = OptionEncoder[T](encoder)
