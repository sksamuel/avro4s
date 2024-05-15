package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.Decoder
import com.sksamuel.avro4s.Encoder
import org.apache.avro.Schema

import scala.jdk.CollectionConverters.*

class OptionDecoder[T](decoder: Decoder[T]) extends Decoder[Option[T]] {

  override def decode(schema: Schema): Any => Option[T] = {
    // nullables must be encoded with a union of 2 elements, one of which is null
    require(schema.getType == Schema.Type.UNION, {
      "Options can only be encoded with a UNION schema"
    })
    require(schema.getTypes.size() >= 2, {
      "An option should be encoded with a UNION schema with at least 2 element types"
    })
    val (isNull, isNotNull) = schema.getTypes().asScala.toList.partition(_.getType() == Schema.Type.NULL)
    require(isNull.size == 1, s"exactly one of the schemas must be null (found ${isNull.size})")
    val elementSchema = isNotNull match {
      case List(single) => single
      case more => Schema.createUnion(more.asJava)
    }

    val decode = decoder.decode(elementSchema)
    { value => if (value == null) None else Some(decode(value)) }
  }
}

trait OptionDecoders:
  given[T](using decoder: Decoder[T]): Decoder[Option[T]] = new OptionDecoder[T](decoder)
