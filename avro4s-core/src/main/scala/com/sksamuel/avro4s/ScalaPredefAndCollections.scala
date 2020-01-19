package com.sksamuel.avro4s

import java.nio.ByteBuffer

import com.sksamuel.avro4s.ScalaPredefAndCollections._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericFixed}

import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

trait ScalaPredefAndCollectionCodecs {

  implicit val ByteArrayCodec: Codec[Array[Byte]] = ScalaPredefAndCollections.ByteArrayCodec

  implicit val NoneCodec: Codec[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionCodec[T](implicit codec: Codec[T]): Codec[Option[T]] = new Codec[Option[T]] {

    val schema: Schema = optionSchema(codec)

    def encode(value: Option[T]): AnyRef = encodeOption(codec, value)

    def decode(value: Any): Option[T] = decodeOption(codec, value)
  }

  implicit def iterableByteCodec[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): Codec[C[Byte]] = new IterableByteCodec[C](cbf)

  def iterableCodec[T, C[X] <: Iterable[X]](implicit codec: Codec[T],
                                            cbf: CanBuildFrom[Nothing, T, C[T]]): Codec[C[T]] =
    new Codec[C[T]] {
      val schema: Schema = SchemaBuilder.array().items(codec.schema)

      def encode(value: C[T]): AnyRef = encodeIterable(codec, value)

      def decode(value: Any): C[T] = decodeIterable(codec, value)
    }
}

trait ScalaPredefAndCollectionEncoders {

  implicit def iterableByteEncoder[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): EncoderV2[C[Byte]] = new IterableByteCodec[C](cbf)

  def iterableEncoder[T, C[X] <: Iterable[X]](implicit encoder: EncoderV2[T]): EncoderV2[C[T]] =
    new EncoderV2[C[T]] {
      val schema: Schema = SchemaBuilder.array().items(encoder.schema)

      def encode(value: C[T]): AnyRef = encodeIterable(encoder, value)
    }
}

trait ScalaPredefAndCollectionDecoders {

  implicit def iterableByteDecoder[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): DecoderV2[C[Byte]] = new IterableByteCodec[C](cbf)

  def iterableDecoder[T, C[X] <: Iterable[X]](implicit decoder: DecoderV2[T],
                                              cbf: CanBuildFrom[Nothing, T, C[T]]): DecoderV2[C[T]] =
    new DecoderV2[C[T]] {
      val schema: Schema = SchemaBuilder.array().items(decoder.schema)

      def decode(value: Any): C[T] = decodeIterable(decoder, value)
    }
}

object ScalaPredefAndCollections {

  private[avro4s] def optionSchema[A[_]](aware: SchemaAware[A, _]): Schema =
    SchemaForV2.optionSchema(SchemaForV2(aware.schema)).schema

  private[avro4s] def encodeOption[T](encoder: EncoderV2[T], value: Option[T]): AnyRef =
    if (value.isEmpty) null else encoder.encode(value.get)

  private[avro4s] def decodeOption[T](decoder: DecoderV2[T], value: Any): Option[T] =
    if (value == null) None else Option(decoder.decode(value))

  object NoneCodec extends Codec[None.type] {
    val schema: Schema = SchemaBuilder.builder.nullType

    def encode(value: None.type): AnyRef = null

    def decode(value: Any): None.type =
      if (value == null) None else sys.error(s"Value $value is not null, but should be decoded to None")
  }

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
        case Schema.Type.FIXED => new FixedByteArrayCodec(schema)
        case _                 => sys.error(s"Byte array codec doesn't support schema type ${schema.getType}")
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

  implicit def iterableByteCodec[C[X] <: Iterable[X]](
      implicit cbf: CanBuildFrom[Nothing, Byte, C[Byte]]): Codec[C[Byte]] = new IterableByteCodec[C](cbf)

  implicit def arrayCodec[T: ClassTag](implicit codec: Codec[T]): Codec[Array[T]] = new Codec[Array[T]] {
    import scala.collection.JavaConverters._

    val schema: Schema = SchemaBuilder.array().items(codec.schema)

    def encode(value: Array[T]): AnyRef = value.map(codec.encode).toList.asJava

    def decode(value: Any): Array[T] = value match {
      case array: Array[_]               => array.map(codec.decode)
      case list: java.util.Collection[_] => list.asScala.map(codec.decode).toArray
      case list: Iterable[_]             => list.map(codec.decode).toArray
      case other                         => sys.error("Unsupported array " + other)
    }
  }

  private[avro4s] def encodeIterable[T, C[X] <: Iterable[X]](encoder: EncoderV2[T], value: C[T]): AnyRef =
    value.map(encoder.encode).toList.asJava

  private[avro4s] def decodeIterable[T, C[X] <: Iterable[X]](decoder: DecoderV2[T], value: Any)(
      implicit cbf: CanBuildFrom[Nothing, T, C[T]]): C[T] = value match {
    case array: Array[_]               => array.map(decoder.decode)(collection.breakOut)
    case list: java.util.Collection[_] => list.asScala.map(decoder.decode)(collection.breakOut)
    case list: Iterable[_]             => list.map(decoder.decode)(collection.breakOut)
    case other                         => sys.error("Unsupported array " + other)
  }
}
