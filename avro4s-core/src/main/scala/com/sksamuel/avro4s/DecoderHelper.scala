package com.sksamuel.avro4s

import com.sksamuel.avro4s.Decoder.Typeclass
import magnolia.Param
import org.apache.avro.Schema

object DecoderHelper {

  def tryDecode[T](fieldMapper: FieldMapper, schema: Schema, param: Param[Typeclass, T], value: Any) = {
    val decodeResult = util.Try {
      param.typeclass.decode(value, schema.getFields.get(param.index).schema, fieldMapper)
    }.toEither
    (decodeResult, param.default) match {
      case (Right(v : Option[_]), Some(default)) if(v.isEmpty) => default
      case (Right(v), _) => v
      case (Left(_), Some(default)) => default
      case (Left(ex), _) => throw ex
    }
  }

}
