package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericFixed}

trait ByteIterableSchemaFors {
  implicit val ByteArraySchemaFor: SchemaFor[Array[Byte]] = SchemaFor[Array[Byte]](SchemaBuilder.builder.bytesType)
  implicit val ByteListSchemaFor: SchemaFor[List[Byte]] = ByteArraySchemaFor.forType
  implicit val ByteSeqSchemaFor: SchemaFor[Seq[Byte]] = ByteArraySchemaFor.forType
  implicit val ByteVectorSchemaFor: SchemaFor[Vector[Byte]] = ByteArraySchemaFor.forType
}

trait ByteIterableDecoders {

  implicit val ByteArrayDecoder: Decoder[Array[Byte]] = new ByteArrayDecoderBase {
    val schemaFor = SchemaFor[Array[Byte]](SchemaBuilder.builder().bytesType())
  }

  implicit val ByteListDecoder: Decoder[List[Byte]] = iterableByteDecoder(_.toList)
  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = iterableByteDecoder(_.toVector)
  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = iterableByteDecoder(_.toSeq)

  private def iterableByteDecoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte]): Decoder[C[Byte]] =
    new IterableByteDecoder[C](build)

  private sealed trait ByteArrayDecoderBase extends Decoder[Array[Byte]] {

    def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer  => buffer.array
      case array: Array[Byte]  => array
      case fixed: GenericFixed => fixed.bytes
      case _                   => sys.error(s"Byte array codec cannot decode '$value'")
    }

    override def withSchema(schemaFor: SchemaFor[Array[Byte]]): Decoder[Array[Byte]] =
      schemaFor.schema.getType match {
        case Schema.Type.BYTES => ByteArrayDecoder
        case Schema.Type.FIXED => new FixedByteArrayDecoder(schemaFor)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schemaFor.schema.getType}")
      }
  }

  private class FixedByteArrayDecoder(val schemaFor: SchemaFor[Array[Byte]]) extends ByteArrayDecoderBase {
    require(schema.getType == Schema.Type.FIXED)
  }

  private class IterableByteDecoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte],
                                                         byteArrayDecoder: Decoder[Array[Byte]] = ByteArrayDecoder)
      extends Decoder[C[Byte]] {

    val schemaFor: SchemaFor[C[Byte]] = byteArrayDecoder.schemaFor.forType

    def decode(value: Any): C[Byte] = build(byteArrayDecoder.decode(value))

    override def withSchema(schemaFor: SchemaFor[C[Byte]]): Decoder[C[Byte]] =
      new IterableByteDecoder(build, byteArrayDecoder.withSchema(schemaFor.map(identity)))
  }
}

trait ByteIterableEncoders {

  implicit val ByteArrayEncoder: Encoder[Array[Byte]] = new ByteArrayEncoderBase {
    val schemaFor = SchemaFor[Array[Byte]](SchemaBuilder.builder().bytesType())
    def encode(value: Array[Byte]): AnyRef = ByteBuffer.wrap(value)
  }

  private def iterableByteEncoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte]): Encoder[C[Byte]] =
    new IterableByteEncoder[C](build)

  implicit val ByteListEncoder: Encoder[List[Byte]] = iterableByteEncoder(_.toList)
  implicit val ByteVectorEncoder: Encoder[Vector[Byte]] = iterableByteEncoder(_.toVector)
  implicit val ByteSeqEncoder: Encoder[Seq[Byte]] = iterableByteEncoder(_.toSeq)

  private sealed trait ByteArrayEncoderBase extends Encoder[Array[Byte]] {
    override def withSchema(schemaFor: SchemaFor[Array[Byte]]): Encoder[Array[Byte]] =
      schemaFor.schema.getType match {
        case Schema.Type.BYTES => ByteArrayEncoder
        case Schema.Type.FIXED => new FixedByteArrayEncoder(schemaFor)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schemaFor.schema.getType}")
      }
  }

  private class FixedByteArrayEncoder(val schemaFor: SchemaFor[Array[Byte]]) extends ByteArrayEncoderBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: Array[Byte]): AnyRef = {
      val array = new Array[Byte](schema.getFixedSize)
      System.arraycopy(value, 0, array, 0, value.length)
      GenericData.get.createFixed(null, array, schema)
    }
  }

  private class IterableByteEncoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte],
                                                         byteArrayEncoder: Encoder[Array[Byte]] = ByteArrayEncoder)
      extends Encoder[C[Byte]] {

    val schemaFor: SchemaFor[C[Byte]] = byteArrayEncoder.schemaFor.forType
    def encode(value: C[Byte]): AnyRef = byteArrayEncoder.encode(value.toArray)

    override def withSchema(schemaFor: SchemaFor[C[Byte]]): Encoder[C[Byte]] =
      new IterableByteEncoder(build, byteArrayEncoder.withSchema(schemaFor.map(identity)))
  }
}
