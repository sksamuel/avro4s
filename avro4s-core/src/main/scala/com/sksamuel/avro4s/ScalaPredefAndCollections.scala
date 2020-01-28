package com.sksamuel.avro4s

import java.util

import com.sksamuel.avro4s.ScalaPredefAndCollections._
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait ScalaPredefAndCollectionCodecs {

  implicit val NoneCodec: Codec[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionCodec[T](implicit codec: Codec[T]): Codec[Option[T]] = new Codec[Option[T]] {

    val schemaFor: SchemaForV2[Option[T]] = SchemaForV2.optionSchema(codec.schemaFor)

    def encode(value: Option[T]): AnyRef = encodeOption(codec, value)

    def decode(value: Any): Option[T] = decodeOption(codec, value)

    override def withSchema(schemaFor: SchemaForV2[Option[T]]): Codec[Option[T]] =
      optionCodec(codec.withSchema(extractOptionSchema(schemaFor)))
  }

  implicit def eitherCodec[A: Manifest: WeakTypeTag, B: Manifest: WeakTypeTag](
      implicit leftCodec: Codec[A],
      rightCodec: Codec[B]): Codec[Either[A, B]] =
    new Codec[Either[A, B]] {
      val schemaFor: SchemaForV2[Either[A, B]] = SchemaForV2.eitherSchema(leftCodec.schemaFor, rightCodec.schemaFor)

      def encode(value: Either[A, B]): AnyRef = encodeEither(value)

      private implicit val leftGuard: PartialFunction[Any, A] = TypeGuardedDecoding.guard(leftCodec)
      private implicit val rightGuard: PartialFunction[Any, B] = TypeGuardedDecoding.guard(rightCodec)

      def decode(value: Any): Either[A, B] = decodeEither(value, manifest[A], manifest[B])

      override def withSchema(schemaFor: SchemaForV2[Either[A, B]]): Codec[Either[A, B]] =
        eitherCodec(
          manifest[A],
          weakTypeTag[A],
          manifest[B],
          weakTypeTag[B],
          leftCodec.withSchema(extractEitherLeftSchema(schemaFor)),
          rightCodec.withSchema(extractEitherRightSchema(schemaFor))
        )
    }

  implicit def arrayCodec[T: ClassTag](implicit codec: Codec[T]): Codec[Array[T]] = new Codec[Array[T]] {
    val schemaFor: SchemaForV2[Array[T]] = SchemaForV2.arraySchema(codec.schemaFor)

    def encode(value: Array[T]): AnyRef = encodeArray(codec, value)

    def decode(value: Any): Array[T] = decodeArray(codec, value)

    override def withSchema(schemaFor: SchemaForV2[Array[T]]): Codec[Array[T]] =
      arrayCodec(implicitly[ClassTag[T]], codec.withSchema(extractArrayElementSchema(schemaFor)))
  }

  private def iterableCodec[C[X] <: Iterable[X], T](codec: Codec[T])(
    implicit builder: Iterable[T] => C[T]): Codec[C[T]] =
    new Codec[C[T]] {
      val schemaFor: SchemaForV2[C[T]] = SchemaForV2.iterableSchema(codec.schemaFor).forType

      def encode(value: C[T]): AnyRef = encodeIterable(codec, value)

      def decode(value: Any): C[T] = decodeIterable(codec, value)

      override def withSchema(schemaFor: SchemaForV2[C[T]]): Codec[C[T]] =
        iterableCodec(codec.withSchema(extractIterableElementSchema(schemaFor)))
    }

  implicit def listCodec[T](implicit codec: Codec[T]): Codec[List[T]] = iterableCodec(codec)(_.toList)
  implicit def mutableSeqCodec[T](implicit codec: Codec[T]): Codec[scala.collection.mutable.Seq[T]] =
    iterableCodec(codec)(_.toBuffer)
  implicit def seqCodec[T](implicit codec: Codec[T]): Codec[Seq[T]] = iterableCodec(codec)(_.toSeq)
  implicit def setCodec[T](implicit codec: Codec[T]): Codec[Set[T]] = iterableCodec(codec)(_.toSet)
  implicit def vectorCodec[T](implicit codec: Codec[T]): Codec[Vector[T]] = iterableCodec(codec)(_.toVector)

  implicit def mapCodec[T](implicit codec: Codec[T]): Codec[Map[String, T]] = new Codec[Map[String, T]] {
    val schemaFor: SchemaForV2[Map[String, T]] = SchemaForV2.mapSchema(codec.schemaFor)

    def encode(value: Map[String, T]): AnyRef = encodeMap(codec, value)

    def decode(value: Any): Map[String, T] = decodeMap(codec, value)

    override def withSchema(schemaFor: SchemaForV2[Map[String, T]]): Codec[Map[String, T]] =
      mapCodec(codec.withSchema(extractMapValueSchema(schemaFor)))
  }
}

trait ScalaPredefAndCollectionEncoders {

  implicit val NoneEncoder: EncoderV2[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Option[T]] = new EncoderV2[Option[T]] {

    val schemaFor: SchemaForV2[Option[T]] = SchemaForV2.optionSchema(encoder.schemaFor)

    def encode(value: Option[T]): AnyRef = encodeOption(encoder, value)

    override def withSchema(schemaFor: SchemaForV2[Option[T]]): EncoderV2[Option[T]] =
      optionEncoder(encoder.withSchema(extractOptionSchema(schemaFor)))
  }

  implicit def eitherEncoder[A, B](implicit leftEncoder: EncoderV2[A],
                                   rightEncoder: EncoderV2[B]): EncoderV2[Either[A, B]] =
    new EncoderV2[Either[A, B]] {
      val schemaFor: SchemaForV2[Either[A, B]] = SchemaForV2.eitherSchema(leftEncoder.schemaFor, rightEncoder.schemaFor)

      def encode(value: Either[A, B]): AnyRef = encodeEither(value)

      override def withSchema(schemaFor: SchemaForV2[Either[A, B]]): EncoderV2[Either[A, B]] =
        eitherEncoder(leftEncoder.withSchema(extractEitherLeftSchema(schemaFor)),
                      rightEncoder.withSchema(extractEitherRightSchema(schemaFor)))
    }

  implicit def arrayEncoder[T: ClassTag](implicit encoder: EncoderV2[T]): EncoderV2[Array[T]] =
    new EncoderV2[Array[T]] {
      val schemaFor: SchemaForV2[Array[T]] = SchemaForV2.arraySchema(encoder.schemaFor)

      def encode(value: Array[T]): AnyRef = encodeArray(encoder, value)

      override def withSchema(schemaFor: SchemaForV2[Array[T]]): EncoderV2[Array[T]] =
        arrayEncoder(implicitly[ClassTag[T]], encoder.withSchema(extractArrayElementSchema(schemaFor)))
    }

  private def iterableEncoder[T, C[X] <: Iterable[X]](encoder: EncoderV2[T]): EncoderV2[C[T]] =
    new EncoderV2[C[T]] {
      val schemaFor: SchemaForV2[C[T]] = SchemaForV2.iterableSchema(encoder.schemaFor).forType

      def encode(value: C[T]): AnyRef = encodeIterable(encoder, value)

      override def withSchema(schemaFor: SchemaForV2[C[T]]): EncoderV2[C[T]] =
        iterableEncoder(encoder.withSchema(extractIterableElementSchema(schemaFor)))
    }

  implicit def listEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[List[T]] = iterableEncoder(encoder)
  implicit def mutableSeqEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[scala.collection.mutable.Seq[T]] =
    iterableEncoder(encoder)
  implicit def seqEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Seq[T]] = iterableEncoder(encoder)
  implicit def setEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Set[T]] = iterableEncoder(encoder)
  implicit def vectorEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Vector[T]] = iterableEncoder(encoder)

  implicit def mapEncoder[T](implicit encoder: EncoderV2[T]): EncoderV2[Map[String, T]] =
    new EncoderV2[Map[String, T]] {
      val schemaFor: SchemaForV2[Map[String, T]] = SchemaForV2.mapSchema(encoder.schemaFor)

      def encode(value: Map[String, T]): AnyRef = encodeMap(encoder, value)

      override def withSchema(schemaFor: SchemaForV2[Map[String, T]]): EncoderV2[Map[String, T]] =
        mapEncoder(encoder.withSchema(extractMapValueSchema(schemaFor)))
    }
}

trait ScalaPredefAndCollectionDecoders {

  implicit val NoneCodec: Decoder[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionDecoder[T](implicit decoder: Decoder[T]): Decoder[Option[T]] = new Decoder[Option[T]] {

    val schemaFor = SchemaForV2.optionSchema(decoder.schemaFor)

    def decode(value: Any): Option[T] = decodeOption(decoder, value)

    override def withSchema(schemaFor: SchemaForV2[Option[T]]): Decoder[Option[T]] =
      optionDecoder(decoder.withSchema(extractOptionSchema(schemaFor)))
  }

  implicit def eitherDecoder[A: Manifest: WeakTypeTag, B: Manifest: WeakTypeTag](
                                                                                  implicit leftDecoder: Decoder[A],
                                                                                  rightDecoder: Decoder[B]): Decoder[Either[A, B]] =
    new Decoder[Either[A, B]] {
      val schemaFor: SchemaForV2[Either[A, B]] = SchemaForV2.eitherSchema(leftDecoder.schemaFor, rightDecoder.schemaFor)

      private implicit val leftGuard: PartialFunction[Any, A] = TypeGuardedDecoding.guard(leftDecoder)
      private implicit val rightGuard: PartialFunction[Any, B] = TypeGuardedDecoding.guard(rightDecoder)

      def decode(value: Any): Either[A, B] = decodeEither(value, manifest[A], manifest[B])

      override def withSchema(schemaFor: SchemaForV2[Either[A, B]]): Decoder[Either[A, B]] =
        eitherDecoder(
          manifest[A],
          weakTypeTag[A],
          manifest[B],
          weakTypeTag[B],
          leftDecoder.withSchema(extractEitherLeftSchema(schemaFor)),
          rightDecoder.withSchema(extractEitherRightSchema(schemaFor))
        )
    }

  implicit def arrayDecoder[T: ClassTag](implicit decoder: Decoder[T]): Decoder[Array[T]] =
    new Decoder[Array[T]] {
      def schemaFor: SchemaForV2[Array[T]] = SchemaForV2.arraySchema(decoder.schemaFor)

      def decode(value: Any): Array[T] = decodeArray(decoder, value)

      override def withSchema(schemaFor: SchemaForV2[Array[T]]): Decoder[Array[T]] =
        arrayDecoder(implicitly[ClassTag[T]], decoder.withSchema(extractArrayElementSchema(schemaFor)))
    }

  private def iterableDecoder[T, C[X] <: Iterable[X]](decoder: Decoder[T])(
    implicit build: Iterable[T] => C[T]): Decoder[C[T]] =
    new Decoder[C[T]] {
      val schemaFor: SchemaForV2[C[T]] = SchemaForV2.iterableSchema(decoder.schemaFor).forType

      def decode(value: Any): C[T] = decodeIterable(decoder, value)

      override def withSchema(schemaFor: SchemaForV2[C[T]]): Decoder[C[T]] =
        iterableDecoder(decoder.withSchema(extractIterableElementSchema(schemaFor)))
    }


  implicit def listDecoder[T](implicit decoder: Decoder[T]): Decoder[List[T]] = iterableDecoder(decoder)(_.toList)
  implicit def mutableSeqDecoder[T](implicit decoder: Decoder[T]): Decoder[scala.collection.mutable.Seq[T]] =
    iterableDecoder(decoder)(_.toBuffer)
  implicit def seqDecoder[T](implicit decoder: Decoder[T]): Decoder[Seq[T]] = iterableDecoder(decoder)(_.toSeq)
  implicit def setDecoder[T](implicit decoder: Decoder[T]): Decoder[Set[T]] = iterableDecoder(decoder)(_.toSet)
  implicit def vectorDecoder[T](implicit decoder: Decoder[T]): Decoder[Vector[T]] = iterableDecoder(decoder)(_.toVector)

  implicit def mapDecoder[T](implicit decoder: Decoder[T]): Decoder[Map[String, T]] =
    new Decoder[Map[String, T]] {
      val schemaFor: SchemaForV2[Map[String, T]] = SchemaForV2.mapSchema(decoder.schemaFor)

      def decode(value: Any): Map[String, T] = decodeMap(decoder, value)

      override def withSchema(schemaFor: SchemaForV2[Map[String, T]]): Decoder[Map[String, T]] =
        mapDecoder(decoder.withSchema(extractMapValueSchema(schemaFor)))
    }
}

object ScalaPredefAndCollections {

  private[avro4s] def extractOptionSchema[T](schemaFor: SchemaForV2[Option[T]]): SchemaForV2[T] = {
    require(schemaFor.schema.getType == Schema.Type.UNION,
            s"Schema type for option encoders / decoders must be UNION, received ${schemaFor.schema.getType}")

    schemaFor.schema.getTypes.asScala.find(_.getType != Schema.Type.NULL) match {
      case Some(s) => SchemaForV2(s, schemaFor.fieldMapper)
      case None    => sys.error(s"Union schema ${schemaFor.schema} doesn't contain any non-null entries")
    }
  }

  @inline
  private[avro4s] def encodeOption[T](encoder: EncoderV2[T], value: Option[T]): AnyRef =
    if (value.isEmpty) null else encoder.encode(value.get)

  @inline
  private[avro4s] def decodeOption[T](decoder: Decoder[T], value: Any): Option[T] =
    if (value == null) None else Option(decoder.decode(value))

  @inline
  private[avro4s] def encodeEither[A, B](value: Either[A, B])(implicit leftEncoder: EncoderV2[A],
                                                              rightEncoder: EncoderV2[B]): AnyRef =
    value match {
      case Left(l)  => leftEncoder.encode(l)
      case Right(r) => rightEncoder.encode(r)
    }

  private[avro4s] def extractEitherLeftSchema[A, B](schemaFor: SchemaForV2[Either[A, B]]): SchemaForV2[A] = {
    require(schemaFor.schema.getType == Schema.Type.UNION,
            s"Schema type for either encoders / decoders must be UNION, received ${schemaFor.schema.getType}")
    require(schemaFor.schema.getTypes.size() == 2,
            s"Schema for either encoders / decoders must be a UNION of to types, received ${schemaFor.schema}")

    SchemaForV2(schemaFor.schema.getTypes.get(0), schemaFor.fieldMapper)
  }

  private[avro4s] def extractEitherRightSchema[A, B](schemaFor: SchemaForV2[Either[A, B]]): SchemaForV2[B] = {
    require(schemaFor.schema.getType == Schema.Type.UNION,
            s"Schema type for either encoders / decoders must be UNION, received ${schemaFor.schema.getType}")
    require(schemaFor.schema.getTypes.size() == 2,
            s"Schema for either encoders / decoders must be a UNION of to types, received ${schemaFor.schema}")

    SchemaForV2(schemaFor.schema.getTypes.get(1), schemaFor.fieldMapper)
  }

  @inline
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
    val schemaFor: SchemaForV2[None.type] = SchemaForV2.noneSchema

    def encode(value: None.type): AnyRef = null

    def decode(value: Any): None.type =
      if (value == null) None else sys.error(s"Value $value is not null, but should be decoded to None")
  }

  private[avro4s] def extractIterableElementSchema[C[X] <: Iterable[X], T](
      schemaFor: SchemaForV2[C[T]]): SchemaForV2[T] = {
    require(
      schemaFor.schema.getType == Schema.Type.ARRAY,
      s"Schema type for list / seq / vector encoders and decoders must be ARRAY, received ${schemaFor.schema.getType}"
    )
    SchemaForV2(schemaFor.schema.getElementType, schemaFor.fieldMapper)
  }

  @inline
  private[avro4s] def encodeIterable[T, C[X] <: Iterable[X]](encoder: EncoderV2[T], value: C[T]): AnyRef =
    value.map(encoder.encode).toList.asJava

  @inline
  private[avro4s] def decodeIterable[T, C[X] <: Iterable[X]](decoder: Decoder[T], value: Any)(
      implicit build: Iterable[T] => C[T]): C[T] = value match {
    case array: Array[_]               => build(array.map(decoder.decode))
    case list: java.util.Collection[_] => build(list.asScala.map(decoder.decode))
    case list: Iterable[_]             => build(list.map(decoder.decode))
    case other                         => sys.error("Unsupported array " + other)
  }

  private[avro4s] def extractArrayElementSchema[T](schemaFor: SchemaForV2[Array[T]]): SchemaForV2[T] = {
    require(schemaFor.schema.getType == Schema.Type.ARRAY,
            s"Schema type for array encoders / decoders must be ARRAY, received ${schemaFor.schema.getType}")
    SchemaForV2(schemaFor.schema.getElementType, schemaFor.fieldMapper)
  }

  @inline
  private[avro4s] def encodeArray[T](encoder: EncoderV2[T], value: Array[T]): AnyRef =
    value.map(encoder.encode).toList.asJava

  @inline
  private[avro4s] def decodeArray[T: ClassTag](decoder: Decoder[T], value: Any): Array[T] = value match {
    case array: Array[_]               => array.map(decoder.decode)
    case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toArray
    case list: Iterable[_]             => list.map(decoder.decode).toArray
    case other                         => sys.error("Unsupported array " + other)
  }

  private[avro4s] def extractMapValueSchema[T](schemaFor: SchemaForV2[Map[String, T]]): SchemaForV2[T] = {
    require(schemaFor.schema.getType == Schema.Type.MAP,
            s"Schema type for map encoders / decoders must be MAP, received ${schemaFor.schema.getType}")
    SchemaForV2(schemaFor.schema.getValueType, schemaFor.fieldMapper)
  }

  @inline
  private[avro4s] def encodeMap[T](encoder: EncoderV2[T], value: Map[String, T]): AnyRef = {
    val map = new util.HashMap[String, AnyRef]
    value.foreach { case (k, v) => map.put(k, encoder.encode(v)) }
    map
  }

  @inline
  private[avro4s] def decodeMap[T](decoder: Decoder[T], value: Any): Map[String, T] = value match {
    case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> decoder.decode(v) }
  }
}
