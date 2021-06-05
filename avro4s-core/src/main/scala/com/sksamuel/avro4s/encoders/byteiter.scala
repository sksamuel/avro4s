package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.{Avro4sConfigurationException, Encoder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.nio.ByteBuffer

trait ByteIterableEncoders:
  given Encoder[ByteBuffer] = ByteBufferEncoder
  given Encoder[Array[Byte]] = ByteArrayEncoder
  val IterableByteEncoder: Encoder[Iterable[Byte]] = ByteArrayEncoder.contramap(_.toArray)
  given Encoder[List[Byte]] = IterableByteEncoder.contramap(_.toIterable)
  given Encoder[Vector[Byte]] = IterableByteEncoder.contramap(_.toIterable)
  given Encoder[Seq[Byte]] = IterableByteEncoder.contramap(_.toIterable)

object ByteBufferEncoder extends Encoder[ByteBuffer] :
  override def encode(schema: Schema): ByteBuffer => Any = {
    schema.getType match {
      case Schema.Type.BYTES => identity
      case Schema.Type.FIXED => FixedByteBufferEncoder.encode(schema)
      case _ => throw new Avro4sConfigurationException(
        s"ByteBufferEncoder doesn't support schema type ${schema.getType}")
    }
  }

object ByteArrayEncoder extends Encoder[Array[Byte]] :
  override def encode(schema: Schema): Array[Byte] => Any = {
    schema.getType match {
      case Schema.Type.BYTES => { bytes => ByteBuffer.wrap(bytes) }
      case Schema.Type.FIXED => FixedByteArrayEncoder.encode(schema)
      case _ => throw new Avro4sConfigurationException(
        s"ByteArrayEncoder doesn't support schema type ${schema.getType}")
    }
  }

object FixedByteBufferEncoder extends Encoder[ByteBuffer] {
  override def encode(schema: Schema): ByteBuffer => Any = { value =>
    val array = new Array[Byte](schema.getFixedSize)
    System.arraycopy(value.array(), 0, array, 0, value.array().length)
    GenericData.get.createFixed(null, array, schema)
  }
}

object FixedByteArrayEncoder extends Encoder[Array[Byte]] {
  override def encode(schema: Schema): Array[Byte] => Any = { value =>
    val array = new Array[Byte](schema.getFixedSize)
    System.arraycopy(value, 0, array, 0, value.length)
    GenericData.get.createFixed(null, array, schema)
  }
}