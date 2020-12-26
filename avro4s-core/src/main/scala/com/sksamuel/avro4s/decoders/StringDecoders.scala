package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util.UUID

trait StringDecoders {
  
  given stringDecoder: Decoder[String] = Decoder {
    case a: String => a.toString
    case a: Array[Byte] => String(a)
    case a: ByteBuffer => String(a.array())
    case a: Utf8 => a.toString
    case a: GenericFixed => String(a.bytes())
  }

  given Decoder[UUID] = stringDecoder.map(UUID.fromString)

}
