package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Avro4sDecodingException, Decoder}
import org.apache.avro.Schema

import java.nio.ByteBuffer

trait ByteDecoders:
  given Decoder[Array[Byte]] = ArrayByteDecoder
  given Decoder[ByteBuffer] = ByteBufferDecoder
  given Decoder[List[Byte]] = ArrayByteDecoder.map(_.toList)
  given Decoder[Seq[Byte]] = ArrayByteDecoder.map(_.toList)
  given Decoder[Vector[Byte]] = ArrayByteDecoder.map(_.toVector)

/**
  * A [[Decoder]] for byte arrays that accepts any compatible type regardless of schema.
  */
object ArrayByteDecoder extends Decoder[Array[Byte]] :
  override def decode(schema: Schema): Any => Array[Byte] = { value =>
    value match {
      case buffer: ByteBuffer => buffer.array
      case array: Array[Byte] => array
      case fixed: org.apache.avro.generic.GenericFixed => fixed.bytes
      case _ => throw new Avro4sDecodingException(s"ArrayByteDecoder cannot decode '$value'", value)
    }
  }

object ByteBufferDecoder extends Decoder[ByteBuffer] :
  override def decode(schema: Schema): Any => ByteBuffer = { value =>
    value match {
      case buffer: ByteBuffer => buffer
      case array: Array[Byte] => ByteBuffer.wrap(array)
      case fixed: org.apache.avro.generic.GenericFixed => ByteBuffer.wrap(fixed.bytes)
      case _ => throw new Avro4sDecodingException(s"ByteBufferDecoder cannot decode '$value'", value)
    }
  }

/**
  * A Strict [[Decoder]] for byte arays that only works if the schema is FIXED.
  */
object FixedByteArrayDecoder extends Decoder[Array[Byte]] :
  override def decode(schema: Schema): Any => Array[Byte] =
    require(schema.getType == Schema.Type.FIXED, {
      s"Fixed byte array decoder only supports schema type FIXED, got $schema"
    })
    { value =>
      value match {
        case fixed: org.apache.avro.generic.GenericFixed => fixed.bytes
        case _ => throw new Avro4sDecodingException(s"FixedByteArrayDecoder cannot decode '$value'", value)
      }
    }