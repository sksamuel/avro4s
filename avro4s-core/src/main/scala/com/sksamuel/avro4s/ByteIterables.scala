package com.sksamuel.avro4s

import java.nio.ByteBuffer

import com.sksamuel.avro4s.ByteIterables._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericFixed}

trait ByteIterableDecoders {

  implicit val ByteArrayDecoder: Decoder[Array[Byte]] = ByteIterables.ByteArrayCodec

  private def iterableByteDecoder[C[X] <: Iterable[X]](
       build: Array[Byte] => C[Byte]): Decoder[C[Byte]] = new IterableByteCodec[C](build)


  implicit val ByteListDecoder: Decoder[List[Byte]] = iterableByteDecoder(_.toList)
  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = iterableByteDecoder(_.toVector)
  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = iterableByteDecoder(_.toSeq)
}

trait ByteIterableEncoders {

  implicit val ByteArrayEncoder: EncoderV2[Array[Byte]] = ByteIterables.ByteArrayCodec

  private def iterableByteEncoder[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte]): EncoderV2[C[Byte]] = new IterableByteCodec[C](build)

  implicit val ByteListEncoder: EncoderV2[List[Byte]] = iterableByteEncoder(_.toList)
  implicit val ByteVectorEncoder: EncoderV2[Vector[Byte]] = iterableByteEncoder(_.toVector)
  implicit val ByteSeqEncoder: EncoderV2[Seq[Byte]] = iterableByteEncoder(_.toSeq)
}

trait ByteIterableCodecs {

  implicit val ByteArrayCodec: Codec[Array[Byte]] = ByteIterables.ByteArrayCodec

  private def iterableByteCodec[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte]): Codec[C[Byte]] = new IterableByteCodec[C](build)

  implicit val ByteListCodec: Codec[List[Byte]] = iterableByteCodec(_.toList)
  implicit val ByteVectorCodec: Codec[Vector[Byte]] = iterableByteCodec(_.toVector)
  implicit val ByteSeqCodec: Codec[Seq[Byte]] = iterableByteCodec(_.toSeq)
}

object ByteIterables {

  private[avro4s] sealed trait ByteArrayCodecBase extends Codec[Array[Byte]] {

    def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer  => buffer.array
      case array: Array[Byte]  => array
      case fixed: GenericFixed => fixed.bytes
      case _                   => sys.error(s"Byte array codec cannot decode '$value'")
    }

    override def withSchema(schemaFor: SchemaForV2[Array[Byte]]): Codec[Array[Byte]] =
      schemaFor.schema.getType match {
        case Schema.Type.BYTES => ByteArrayCodec
        case Schema.Type.FIXED => new FixedByteArrayCodec(schemaFor)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schemaFor.schema.getType}")
      }
  }

  val ByteArrayCodec: Codec[Array[Byte]] = new ByteArrayCodecBase {

    val schemaFor = SchemaForV2.arraySchema[Byte]

    def encode(value: Array[Byte]): AnyRef = ByteBuffer.wrap(value)
  }

  private[avro4s] class FixedByteArrayCodec(val schemaFor: SchemaForV2[Array[Byte]]) extends ByteArrayCodecBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: Array[Byte]): AnyRef = {
      val array = new Array[Byte](schema.getFixedSize)
      System.arraycopy(value, 0, array, 0, value.length)
      GenericData.get.createFixed(null, array, schema)
    }
  }

  private[avro4s] class IterableByteCodec[C[X] <: Iterable[X]](build: Array[Byte] => C[Byte],
                                                               byteArrayCodec: Codec[Array[Byte]] = ByteArrayCodec)
      extends Codec[C[Byte]] {

    val schemaFor: SchemaForV2[C[Byte]] = byteArrayCodec.schemaFor.forType

    def encode(value: C[Byte]): AnyRef = byteArrayCodec.encode(value.toArray)

    def decode(value: Any): C[Byte] = build(byteArrayCodec.decode(value))

    override def withSchema(schemaFor: SchemaForV2[C[Byte]]): Codec[C[Byte]] =
      new IterableByteCodec(build, byteArrayCodec.withSchema(schemaFor.map(identity)))
  }
}
