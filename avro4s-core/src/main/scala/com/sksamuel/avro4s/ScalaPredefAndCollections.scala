package com.sksamuel.avro4s

import java.util

import com.sksamuel.avro4s.ScalaPredefAndCollections._
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait ScalaPredefAndCollectionEncoders {

  implicit val NoneEncoder: Encoder[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionEncoder[T](implicit encoder: Encoder[T]): Encoder[Option[T]] = new Encoder[Option[T]] {

    val schemaFor: SchemaFor[Option[T]] = SchemaFor.optionSchemaFor(encoder.schemaFor)

    def encode(value: Option[T]): AnyRef = if (value.isEmpty) null else encoder.encode(value.get)

    override def withSchema(schemaFor: SchemaFor[Option[T]]): Encoder[Option[T]] =
      optionEncoder(encoder.withSchema(extractOptionSchema(schemaFor)))
  }

  implicit def eitherEncoder[A, B](implicit leftEncoder: Encoder[A], rightEncoder: Encoder[B]): Encoder[Either[A, B]] =
    new Encoder[Either[A, B]] {
      val schemaFor: SchemaFor[Either[A, B]] = SchemaFor.eitherSchemaFor(leftEncoder.schemaFor, rightEncoder.schemaFor)

      def encode(value: Either[A, B]): AnyRef = value match {
        case Left(l)  => leftEncoder.encode(l)
        case Right(r) => rightEncoder.encode(r)
      }

      override def withSchema(schemaFor: SchemaFor[Either[A, B]]): Encoder[Either[A, B]] =
        eitherEncoder(leftEncoder.withSchema(extractEitherLeftSchema(schemaFor)),
                      rightEncoder.withSchema(extractEitherRightSchema(schemaFor)))
    }

  implicit def arrayEncoder[T: ClassTag](implicit encoder: Encoder[T]): Encoder[Array[T]] =
    new Encoder[Array[T]] {
      val schemaFor: SchemaFor[Array[T]] = SchemaFor.arraySchemaFor(encoder.schemaFor)

      def encode(value: Array[T]): AnyRef = value.map(encoder.encode).toList.asJava

      override def withSchema(schemaFor: SchemaFor[Array[T]]): Encoder[Array[T]] =
        arrayEncoder(implicitly[ClassTag[T]], encoder.withSchema(extractArrayElementSchema(schemaFor)))
    }

  private def iterableEncoder[T, C[X] <: Iterable[X]](encoder: Encoder[T]): Encoder[C[T]] =
    new Encoder[C[T]] {
      val schemaFor: SchemaFor[C[T]] = SchemaFor.iterableSchemaFor(encoder.schemaFor).forType

      def encode(value: C[T]): AnyRef = value.map(encoder.encode).toList.asJava

      override def withSchema(schemaFor: SchemaFor[C[T]]): Encoder[C[T]] =
        iterableEncoder(encoder.withSchema(extractIterableElementSchema(schemaFor)))
    }

  implicit def listEncoder[T](implicit encoder: Encoder[T]): Encoder[List[T]] = iterableEncoder(encoder)
  implicit def mutableSeqEncoder[T](implicit encoder: Encoder[T]): Encoder[scala.collection.mutable.Seq[T]] =
    iterableEncoder(encoder)
  implicit def seqEncoder[T](implicit encoder: Encoder[T]): Encoder[Seq[T]] = iterableEncoder(encoder)
  implicit def setEncoder[T](implicit encoder: Encoder[T]): Encoder[Set[T]] = iterableEncoder(encoder)
  implicit def vectorEncoder[T](implicit encoder: Encoder[T]): Encoder[Vector[T]] = iterableEncoder(encoder)

  implicit def mapEncoder[T](implicit encoder: Encoder[T]): Encoder[Map[String, T]] =
    new Encoder[Map[String, T]] {
      val schemaFor: SchemaFor[Map[String, T]] = SchemaFor.mapSchemaFor(encoder.schemaFor)

      def encode(value: Map[String, T]): AnyRef = {
        val map = new util.HashMap[String, AnyRef]
        value.foreach { case (k, v) => map.put(k, encoder.encode(v)) }
        map
      }

      override def withSchema(schemaFor: SchemaFor[Map[String, T]]): Encoder[Map[String, T]] =
        mapEncoder(encoder.withSchema(extractMapValueSchema(schemaFor)))
    }
}

trait ScalaPredefAndCollectionDecoders {

  implicit val NoneCodec: Decoder[None.type] = ScalaPredefAndCollections.NoneCodec

  implicit def optionDecoder[T](implicit decoder: Decoder[T]): Decoder[Option[T]] = new Decoder[Option[T]] {

    val schemaFor = SchemaFor.optionSchemaFor(decoder.schemaFor)

    def decode(value: Any): Option[T] = if (value == null) None else Option(decoder.decode(value))

    override def withSchema(schemaFor: SchemaFor[Option[T]]): Decoder[Option[T]] =
      optionDecoder(decoder.withSchema(extractOptionSchema(schemaFor)))
  }

  implicit def eitherDecoder[A: Manifest: WeakTypeTag, B: Manifest: WeakTypeTag](
      implicit leftDecoder: Decoder[A],
      rightDecoder: Decoder[B]): Decoder[Either[A, B]] =
    new Decoder[Either[A, B]] {
      val schemaFor: SchemaFor[Either[A, B]] = SchemaFor.eitherSchemaFor(leftDecoder.schemaFor, rightDecoder.schemaFor)

      private implicit val leftGuard: PartialFunction[Any, A] = TypeGuardedDecoding.guard(leftDecoder)
      private implicit val rightGuard: PartialFunction[Any, B] = TypeGuardedDecoding.guard(rightDecoder)

      def decode(value: Any): Either[A, B] =
        if (leftGuard.isDefinedAt(value)) {
          Left(leftGuard(value))
        } else if (rightGuard.isDefinedAt(value)) {
          Right(rightGuard(value))
        } else {
          val nameA = NameExtractor(manifest[A].runtimeClass).fullName
          val nameB = NameExtractor(manifest[B].runtimeClass).fullName
          sys.error(s"Could not decode $value into Either[$nameA, $nameB]")
        }

      override def withSchema(schemaFor: SchemaFor[Either[A, B]]): Decoder[Either[A, B]] =
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
      def schemaFor: SchemaFor[Array[T]] = SchemaFor.arraySchemaFor(decoder.schemaFor)

      def decode(value: Any): Array[T] = value match {
        case array: Array[_]               => array.map(decoder.decode)
        case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toArray
        case list: Iterable[_]             => list.map(decoder.decode).toArray
        case other                         => sys.error("Unsupported array " + other)
      }

      override def withSchema(schemaFor: SchemaFor[Array[T]]): Decoder[Array[T]] =
        arrayDecoder(implicitly[ClassTag[T]], decoder.withSchema(extractArrayElementSchema(schemaFor)))
    }

  private def iterableDecoder[T, C[X] <: Iterable[X]](decoder: Decoder[T])(
      implicit build: Iterable[T] => C[T]): Decoder[C[T]] =
    new Decoder[C[T]] {
      val schemaFor: SchemaFor[C[T]] = SchemaFor.iterableSchemaFor(decoder.schemaFor).forType

      def decode(value: Any): C[T] = value match {
        case list: java.util.Collection[_] => build(list.asScala.map(decoder.decode))
        case list: Iterable[_]             => build(list.map(decoder.decode))
        case array: Array[_] =>
          // converting array to Seq in order to avoid requiring ClassTag[T] as does arrayDecoder.
          build(array.toSeq.map(decoder.decode))
        case other => sys.error("Unsupported array " + other)
      }

      override def withSchema(schemaFor: SchemaFor[C[T]]): Decoder[C[T]] =
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
      val schemaFor: SchemaFor[Map[String, T]] = SchemaFor.mapSchemaFor(decoder.schemaFor)

      def decode(value: Any): Map[String, T] = value match {
        case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> decoder.decode(v) }
      }

      override def withSchema(schemaFor: SchemaFor[Map[String, T]]): Decoder[Map[String, T]] =
        mapDecoder(decoder.withSchema(extractMapValueSchema(schemaFor)))
    }
}

object ScalaPredefAndCollections {

  private[avro4s] def extractOptionSchema[T](schemaFor: SchemaFor[Option[T]]): SchemaFor[T] = {
    require(schemaFor.schema.getType == Schema.Type.UNION,
            s"Schema type for option encoders / decoders must be UNION, received ${schemaFor.schema.getType}")

    schemaFor.schema.getTypes.asScala.find(_.getType != Schema.Type.NULL) match {
      case Some(s) => SchemaFor(s, schemaFor.fieldMapper)
      case None    => sys.error(s"Union schema ${schemaFor.schema} doesn't contain any non-null entries")
    }
  }

  private[avro4s] def extractEitherLeftSchema[A, B](schemaFor: SchemaFor[Either[A, B]]): SchemaFor[A] = {
    validateEitherSchema(schemaFor)
    SchemaFor(schemaFor.schema.getTypes.get(0), schemaFor.fieldMapper)
  }

  private[avro4s] def extractEitherRightSchema[A, B](schemaFor: SchemaFor[Either[A, B]]): SchemaFor[B] = {
    validateEitherSchema(schemaFor)
    SchemaFor(schemaFor.schema.getTypes.get(1), schemaFor.fieldMapper)
  }

  private[avro4s] def validateEitherSchema[A, B](schemaFor: SchemaFor[Either[A, B]]): Unit = {
    require(schemaFor.schema.getType == Schema.Type.UNION,
            s"Schema type for either encoders / decoders must be UNION, received ${schemaFor.schema.getType}")
    require(schemaFor.schema.getTypes.size() == 2,
            s"Schema for either encoders / decoders must be a UNION of to types, received ${schemaFor.schema}")
  }

  object NoneCodec extends Codec[None.type] {
    val schemaFor: SchemaFor[None.type] = SchemaFor.noneSchemaFor

    def encode(value: None.type): AnyRef = null

    def decode(value: Any): None.type =
      if (value == null) None else sys.error(s"Value $value is not null, but should be decoded to None")
  }

  private[avro4s] def extractIterableElementSchema[C[X] <: Iterable[X], T](schemaFor: SchemaFor[C[T]]): SchemaFor[T] = {
    require(
      schemaFor.schema.getType == Schema.Type.ARRAY,
      s"Schema type for list / seq / vector encoders and decoders must be ARRAY, received ${schemaFor.schema.getType}"
    )
    SchemaFor(schemaFor.schema.getElementType, schemaFor.fieldMapper)
  }

  private[avro4s] def extractArrayElementSchema[T](schemaFor: SchemaFor[Array[T]]): SchemaFor[T] = {
    require(schemaFor.schema.getType == Schema.Type.ARRAY,
            s"Schema type for array encoders / decoders must be ARRAY, received ${schemaFor.schema.getType}")
    SchemaFor(schemaFor.schema.getElementType, schemaFor.fieldMapper)
  }

  private[avro4s] def extractMapValueSchema[T](schemaFor: SchemaFor[Map[String, T]]): SchemaFor[T] = {
    require(schemaFor.schema.getType == Schema.Type.MAP,
            s"Schema type for map encoders / decoders must be MAP, received ${schemaFor.schema.getType}")
    SchemaFor(schemaFor.schema.getValueType, schemaFor.fieldMapper)
  }
}
