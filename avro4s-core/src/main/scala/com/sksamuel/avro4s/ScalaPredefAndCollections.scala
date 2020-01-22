package com.sksamuel.avro4s

import com.sksamuel.avro4s.ScalaPredefAndCollections._
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait ScalaPredefAndCollectionCodecs {

  implicit val NoneCodec: Codec[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionCodec[T](implicit codec: Codec[T]): Codec[Option[T]] = new Codec[Option[T]] {

    val schema: Schema = optionSchema(codec)

    def encode(value: Option[T]): AnyRef = encodeOption(codec, value)

    def decode(value: Any): Option[T] = decodeOption(codec, value)
  }

  implicit def eitherCodec[A: Manifest: WeakTypeTag, B: Manifest: WeakTypeTag](
      implicit leftCodec: Codec[A],
      rightCodec: Codec[B]): Codec[Either[A, B]] =
    new Codec[Either[A, B]] {
      val schema: Schema = SchemaHelper.createSafeUnion(leftCodec.schema, rightCodec.schema)

      def encode(value: Either[A, B]): AnyRef = encodeEither(value)

      private implicit val leftGuard: PartialFunction[Any, A] = TypeGuardedDecoding.guard(leftCodec)
      private implicit val rightGuard: PartialFunction[Any, B] = TypeGuardedDecoding.guard(rightCodec)

      def decode(value: Any): Either[A, B] = decodeEither(value, manifest[A], manifest[B])
    }

  private implicit def iterableCodec[C[X] <: Iterable[X], T](codec: Codec[T])(
      implicit cbf: CanBuildFrom[Nothing, T, C[T]]): Codec[C[T]] =
    new Codec[C[T]] {
      val schema: Schema = iterableSchema(codec)

      def encode(value: C[T]): AnyRef = encodeIterable(codec, value)

      def decode(value: Any): C[T] = decodeIterable(codec, value)
    }

  implicit def arrayCodec[T: ClassTag](implicit codec: Codec[T]): Codec[Array[T]] = new Codec[Array[T]] {
    def schema: Schema = arraySchema(codec)

    def encode(value: Array[T]): AnyRef = encodeArray(codec, value)

    def decode(value: Any): Array[T] = decodeArray(codec, value)
  }

  implicit def listCodec[T](implicit codec: Codec[T]): Codec[List[T]] = iterableCodec(codec)
  implicit def mapCodec[T](implicit codec: Codec[T]): Codec[Map[String, T]] = ???
  implicit def mutableSeqCodec[T](implicit codec: Codec[T]): Codec[scala.collection.mutable.Seq[T]] =
    iterableCodec(codec)
  implicit def seqCodec[T](implicit codec: Codec[T]): Codec[Seq[T]] = iterableCodec(codec)
  implicit def setCodec[T](implicit codec: Codec[T]): Codec[Set[T]] = iterableCodec(codec)
  implicit def vectorCodec[T](implicit codec: Codec[T]): Codec[Vector[T]] = iterableCodec(codec)
}

trait ScalaPredefAndCollectionEncoders {

  implicit val NoneEncoder: EncoderV2[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Option[T]] = new EncoderV2[Option[T]] {

    val schema: Schema = optionSchema(encoder)

    def encode(value: Option[T]): AnyRef = encodeOption(encoder, value)
  }

  implicit def eitherEncoder[A, B](implicit leftEncoder: EncoderV2[A],
                                   rightEncoder: EncoderV2[B]): EncoderV2[Either[A, B]] =
    new EncoderV2[Either[A, B]] {
      val schema: Schema = SchemaHelper.createSafeUnion(leftEncoder.schema, rightEncoder.schema)

      def encode(value: Either[A, B]): AnyRef = encodeEither(value)
    }

  private def iterableEncoder[T, C[X] <: Iterable[X]](encoder: EncoderV2[T]): EncoderV2[C[T]] =
    new EncoderV2[C[T]] {
      val schema: Schema = iterableSchema(encoder)

      def encode(value: C[T]): AnyRef = encodeIterable(encoder, value)
    }

  implicit def arrayEncoder[T: ClassTag](implicit encoder: EncoderV2[T]): EncoderV2[Array[T]] =
    new EncoderV2[Array[T]] {
      def schema: Schema = arraySchema(encoder)

      def encode(value: Array[T]): AnyRef = encodeArray(encoder, value)
    }

  implicit def listEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[List[T]] = iterableEncoder(encoder)
  implicit def mapEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Map[String, T]] = ???
  implicit def mutableSeqEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[scala.collection.mutable.Seq[T]] =
    iterableEncoder(encoder)
  implicit def seqEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Seq[T]] = iterableEncoder(encoder)
  implicit def setEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Set[T]] = iterableEncoder(encoder)
  implicit def vectorEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Vector[T]] = iterableEncoder(encoder)
}

trait ScalaPredefAndCollectionDecoders {

  implicit val NoneCodec: DecoderV2[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionDecoder[T](implicit decoder: DecoderV2[T]): DecoderV2[Option[T]] = new DecoderV2[Option[T]] {

    val schema: Schema = optionSchema(decoder)

    def decode(value: Any): Option[T] = decodeOption(decoder, value)
  }

  implicit def eitherDecoder[A: Manifest: WeakTypeTag, B: Manifest: WeakTypeTag](
      implicit leftDecoder: DecoderV2[A],
      rightDecoder: DecoderV2[B]): DecoderV2[Either[A, B]] =
    new DecoderV2[Either[A, B]] {
      val schema: Schema = SchemaHelper.createSafeUnion(leftDecoder.schema, rightDecoder.schema)

      private implicit val leftGuard: PartialFunction[Any, A] = TypeGuardedDecoding.guard(leftDecoder)
      private implicit val rightGuard: PartialFunction[Any, B] = TypeGuardedDecoding.guard(rightDecoder)

      def decode(value: Any): Either[A, B] = decodeEither(value, manifest[A], manifest[B])
    }

  private def iterableDecoder[T, C[X] <: Iterable[X]](decoder: DecoderV2[T])(
      implicit cbf: CanBuildFrom[Nothing, T, C[T]]): DecoderV2[C[T]] =
    new DecoderV2[C[T]] {
      val schema: Schema = iterableSchema(decoder)

      def decode(value: Any): C[T] = decodeIterable(decoder, value)
    }

  implicit def arrayDecoder[T: ClassTag](implicit decoder: DecoderV2[T]): DecoderV2[Array[T]] =
    new DecoderV2[Array[T]] {
      def schema: Schema = arraySchema(decoder)

      def decode(value: Any): Array[T] = decodeArray(decoder, value)
    }

  implicit def listDecoder[T](implicit decoder: DecoderV2[T]): DecoderV2[List[T]] = iterableDecoder(decoder)
  implicit def mapDecoder[T](implicit decoder: DecoderV2[T]): DecoderV2[Map[String, T]] = ???
  implicit def mutableSeqDecoder[T](implicit decoder: DecoderV2[T]): DecoderV2[scala.collection.mutable.Seq[T]] =
    iterableDecoder(decoder)
  implicit def seqDecoder[T](implicit decoder: DecoderV2[T]): DecoderV2[Seq[T]] = iterableDecoder(decoder)
  implicit def setDecoder[T](implicit decoder: DecoderV2[T]): DecoderV2[Set[T]] = iterableDecoder(decoder)
  implicit def vectorDecoder[T](implicit decoder: DecoderV2[T]): DecoderV2[Vector[T]] = iterableDecoder(decoder)
}

object ScalaPredefAndCollections {

  private[avro4s] def optionSchema[A[_]](aware: SchemaAware[A, _]): Schema =
    SchemaForV2.optionSchema(SchemaForV2(aware.schema)).schema

  private[avro4s] def encodeOption[T](encoder: EncoderV2[T], value: Option[T]): AnyRef =
    if (value.isEmpty) null else encoder.encode(value.get)

  private[avro4s] def decodeOption[T](decoder: DecoderV2[T], value: Any): Option[T] =
    if (value == null) None else Option(decoder.decode(value))

  private[avro4s] def encodeEither[A, B](value: Either[A, B])(implicit leftEncoder: EncoderV2[A],
                                                              rightEncoder: EncoderV2[B]): AnyRef =
    value match {
      case Left(l)  => leftEncoder.encode(l)
      case Right(r) => rightEncoder.encode(r)
    }

  private[avro4s] def decodeEither[A, B](value: Any, manifestA: Manifest[A], manifestB: Manifest[B])(
      implicit leftGuard: PartialFunction[Any, A],
      rightGuard: PartialFunction[Any, B]): Either[A, B] =
    if (leftGuard.isDefinedAt(value)) {
      Left(leftGuard(value))
    } else if (rightGuard.isDefinedAt(value)) {
      Right(rightGuard(value))
    } else {
      val nameA = NameExtractor(manifestA.runtimeClass).fullName
      val nameB = NameExtractor(manifestB.runtimeClass).fullName
      sys.error(s"Could not decode $value into Either[$nameA, $nameB]")
    }

  object NoneCodec extends Codec[None.type] {
    val schema: Schema = SchemaBuilder.builder.nullType

    def encode(value: None.type): AnyRef = null

    def decode(value: Any): None.type =
      if (value == null) None else sys.error(s"Value $value is not null, but should be decoded to None")
  }

  private[avro4s] def iterableSchema[TC[_], T](itemSchemaAware: SchemaAware[TC, T]): Schema =
    SchemaBuilder.array().items(itemSchemaAware.schema)

  private[avro4s] def encodeIterable[T, C[X] <: Iterable[X]](encoder: EncoderV2[T], value: C[T]): AnyRef =
    value.map(encoder.encode).toList.asJava

  private[avro4s] def decodeIterable[T, C[X] <: Iterable[X]](decoder: DecoderV2[T], value: Any)(
      implicit cbf: CanBuildFrom[Nothing, T, C[T]]): C[T] = value match {
    case array: Array[_]               => array.map(decoder.decode)(collection.breakOut)
    case list: java.util.Collection[_] => list.asScala.map(decoder.decode)(collection.breakOut)
    case list: Iterable[_]             => list.map(decoder.decode)(collection.breakOut)
    case other                         => sys.error("Unsupported array " + other)
  }

  private[avro4s] def arraySchema[TC[_], T](itemSchemaAware: SchemaAware[TC, T]): Schema =
    SchemaBuilder.array().items(itemSchemaAware.schema)

  private[avro4s] def encodeArray[T](encoder: EncoderV2[T], value: Array[T]): AnyRef =
    value.map(encoder.encode).toList.asJava

  private[avro4s] def decodeArray[T: ClassTag](decoder: DecoderV2[T], value: Any): Array[T] = value match {
    case array: Array[_]               => array.map(decoder.decode)
    case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toArray
    case list: Iterable[_]             => list.map(decoder.decode).toArray
    case other                         => sys.error("Unsupported array " + other)
  }
}
