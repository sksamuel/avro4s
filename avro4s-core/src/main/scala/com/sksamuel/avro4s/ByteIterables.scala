package com.sksamuel.avro4s

import java.nio.ByteBuffer

import com.sksamuel.avro4s.ByteIterables._
import com.sksamuel.avro4s.Decoder.ByteArrayDecoder
import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.generic.CanBuildFrom

trait ByteIterableDecoders {

  implicit val ByteArrayDecoder: DecoderV2[Array[Byte]] = ByteIterables.ByteArrayCodec

  private def iterableByteDecoder[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): DecoderV2[C[Byte]] = new IterableByteCodec[C](cbf)


  implicit val ByteListDecoder: DecoderV2[List[Byte]] = iterableByteDecoder
  implicit val ByteVectorDecoder: DecoderV2[Vector[Byte]] = iterableByteDecoder
  implicit val ByteSeqDecoder: DecoderV2[Seq[Byte]] = iterableByteDecoder
}

trait ByteIterableEncoders {

  implicit val ByteArrayEncoder: EncoderV2[Array[Byte]] = ByteIterables.ByteArrayCodec

  implicit def iterableByteEncoder[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): EncoderV2[C[Byte]] = new IterableByteCodec[C](cbf)

  implicit val ByteListEncoder: EncoderV2[List[Byte]] = iterableByteEncoder
  implicit val ByteVectorEncoder: EncoderV2[Vector[Byte]] = iterableByteEncoder
  implicit val ByteSeqEncoder: EncoderV2[Seq[Byte]] = iterableByteEncoder
}

trait ByteIterableCodecs {

  implicit val ByteArrayCodec: Codec[Array[Byte]] = ByteIterables.ByteArrayCodec

  implicit def iterableByteCodec[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): Codec[C[Byte]] = new IterableByteCodec[C](cbf)

  implicit val ByteListCodec: Codec[List[Byte]] = iterableByteCodec
  implicit val ByteVectorCodec: Codec[Vector[Byte]] = iterableByteCodec
  implicit val ByteSeqCodec: Codec[Seq[Byte]] = iterableByteCodec
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
        case Schema.Type.FIXED => new FixedByteArrayCodec(schemaFor.schema)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schemaFor.schema.getType}")
      }
  }

  val ByteArrayCodec: Codec[Array[Byte]] = new ByteArrayCodecBase {

    val schema: Schema = SchemaBuilder.builder.bytesType

    def encode(value: Array[Byte]): AnyRef = ByteBuffer.wrap(value)
  }

  private[avro4s] class FixedByteArrayCodec(val schema: Schema) extends ByteArrayCodecBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: Array[Byte]): AnyRef = {
      val array = new Array[Byte](schema.getFixedSize)
      System.arraycopy(value, 0, array, 0, value.length)
      GenericData.get.createFixed(null, array, schema)
    }
  }

  private[avro4s] class IterableByteCodec[C[X] <: Iterable[X]](cbf: CanBuildFrom[Nothing, Byte, C[Byte]],
                                                               byteArrayCodec: Codec[Array[Byte]] = ByteArrayCodec)
      extends Codec[C[Byte]] {

    val schema = byteArrayCodec.schema

    def encode(value: C[Byte]): AnyRef = byteArrayCodec.encode(value.toArray)

    def decode(value: Any): C[Byte] = byteArrayCodec.decode(value).to[C](cbf)

    override def withSchema(schemaFor: SchemaForV2[C[Byte]]): Codec[C[Byte]] =
      new IterableByteCodec(cbf, byteArrayCodec.withSchema(schemaFor.map(identity)))
  }

  private[avro4s] def iterableByteCodec[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): Codec[C[Byte]] = new IterableByteCodec[C](cbf)

}
