package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.FieldMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util.UUID

trait PrimitiveDecoders {

  given Decoder[Long] = Decoder {
    case a: Long => a
    case a: Int => a.toLong
    case a: Short => a.toLong
    case a: Byte => a.toLong
  }

  given Decoder[Int] = Decoder {
    case a: Int => a
    case a: Short => a.toInt
    case a: Byte => a.toInt
  }

  given Decoder[Boolean] = Decoder {
    case a: Boolean => a
  }

  given Decoder[Double] = Decoder {
    case a: Double => a
    case a: Float => a
  }

  given Decoder[Float] = Decoder {
    case a: Float => a
  }

  given Decoder[ByteBuffer] = Decoder {
    case a: ByteBuffer => a
    case a: Array[Byte] => ByteBuffer.wrap(a)
  }
}
