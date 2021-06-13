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