package com.sksamuel.avro4s

import java.util

import com.sksamuel.avro4s.CollectionsAndContainers._

import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait CollectionAndContainerSchemaFors {

  implicit val noneSchemaFor: SchemaFor[None.type] = CollectionsAndContainers.noneSchemaFor

  implicit def optionSchemaForRec[T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[Option[T]] =
    (env, update) => optionSchemaFor(itemSFR(env, update))

  implicit def eitherSchemaForRec[A, B](implicit leftSFR: SchemaFor[A],
                                        rightSFR: SchemaFor[B]): ResolvableSchemaFor[Either[A, B]] =
    (env, update) => eitherSchemaFor(leftSFR(env, update), rightSFR(env, update))

  private def _iterableSchemaForRec[C[X] <: Iterable[X], T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[C[T]] =
    (env, update) => iterableSchemaFor(itemSFR(env, update))

  implicit def arraySchemaForRec[T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[Array[T]] = { (env, update) =>
    itemSFR(env, update).map(SchemaBuilder.array.items(_))
  }

  implicit def iterableSchemaForRec[T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[Iterable[T]] =
    _iterableSchemaForRec[Iterable, T](itemSFR)

  implicit def listSchemaForRec[T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[List[T]] =
    _iterableSchemaForRec[List, T](itemSFR)

  implicit def setSchemaForRec[T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[Set[T]] =
    _iterableSchemaForRec[Set, T](itemSFR)

  implicit def vectorSchemaForRec[T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[Vector[T]] =
    _iterableSchemaForRec[Vector, T](itemSFR)

  implicit def seqSchemaForRec[T](implicit itemSFR: SchemaFor[T]): ResolvableSchemaFor[Seq[T]] =
    _iterableSchemaForRec[Seq, T](itemSFR)

  implicit def mapSchemaForRec[T](implicit valueSFR: SchemaFor[T]): ResolvableSchemaFor[Map[String, T]] =
    (env, update) => mapSchemaFor(valueSFR(env, update))
}

trait CollectionAndContainerEncoders {

  import EncoderHelpers._

  implicit val NoneEncoder: Encoder[None.type] = new Encoder[None.type] {
    val schemaFor: SchemaFor[None.type] = noneSchemaFor
    def encode(value: None.type): AnyRef = null
  }

  implicit def optionEncoder[T](implicit valueER: Encoder[T]): UnresolvedEncoder[Option[T]] = { (env, update) =>
    val encoder = valueER(env, mapFullUpdate(extractOptionSchema, update))

    new Encoder[Option[T]] {

      val schemaFor: SchemaFor[Option[T]] = optionSchemaFor(encoder.schemaFor)

      def encode(value: Option[T]): AnyRef = if (value.isEmpty) null else encoder.encode(value.get)

      override def withSchema(schemaFor: SchemaFor[Option[T]]): Encoder[Option[T]] =
        buildWithSchema(optionEncoder(valueER), schemaFor)
    }
  }

  implicit def eitherEncoder[A, B](implicit leftER: Encoder[A],
                                   rightER: Encoder[B]): UnresolvedEncoder[Either[A, B]] = { (env, update) =>
    val leftEncoder = leftER(env, mapFullUpdate(extractEitherLeftSchema, update))
    val rightEncoder = rightER(env, mapFullUpdate(extractEitherRightSchema, update))

    new Encoder[Either[A, B]] {
      val schemaFor: SchemaFor[Either[A, B]] = eitherSchemaFor(leftEncoder.schemaFor, rightEncoder.schemaFor)

      def encode(value: Either[A, B]): AnyRef = value match {
        case Left(l)  => leftEncoder.encode(l)
        case Right(r) => rightEncoder.encode(r)
      }

      override def withSchema(schemaFor: SchemaFor[Either[A, B]]): Encoder[Either[A, B]] =
        buildWithSchema(eitherEncoder(leftER, rightER), schemaFor)
    }
  }

  implicit def arrayEncoder[T: ClassTag](implicit elemencoderRes: Encoder[T]): UnresolvedEncoder[Array[T]] = {
    (env, update) =>
      val encoder = elemencoderRes(env, mapFullUpdate(extractIterableElementSchema, update))

      new Encoder[Array[T]] {
        val schemaFor: SchemaFor[Array[T]] = iterableSchemaFor(encoder.schemaFor).forType

        def encode(value: Array[T]): AnyRef = value.map(encoder.encode).toList.asJava

        override def withSchema(schemaFor: SchemaFor[Array[T]]): Encoder[Array[T]] =
          buildWithSchema(arrayEncoder(implicitly[ClassTag[T]], elemencoderRes), schemaFor)
      }
  }

  private def iterableEncoder[T, C[X] <: Iterable[X]](elemencoderRes: Encoder[T]): UnresolvedEncoder[C[T]] = {
    (env, update) =>
      val encoder = elemencoderRes(env, mapFullUpdate(extractIterableElementSchema, update))

      new Encoder[C[T]] {
        val schemaFor: SchemaFor[C[T]] = iterableSchemaFor(encoder.schemaFor)

        def encode(value: C[T]): AnyRef = value.map(encoder.encode).toList.asJava

        override def withSchema(schemaFor: SchemaFor[C[T]]): Encoder[C[T]] =
          buildWithSchema(iterableEncoder(elemencoderRes), schemaFor)
      }
  }

  implicit def listEncoder[T](implicit encoder: Encoder[T]): Encoder[List[T]] = iterableEncoder(encoder)
  implicit def mutableSeqEncoder[T](implicit encoder: Encoder[T]): Encoder[scala.collection.mutable.Seq[T]] =
    iterableEncoder(encoder)
  implicit def seqEncoder[T](implicit encoder: Encoder[T]): Encoder[Seq[T]] = iterableEncoder(encoder)
  implicit def setEncoder[T](implicit encoder: Encoder[T]): Encoder[Set[T]] = iterableEncoder(encoder)
  implicit def vectorEncoder[T](implicit encoder: Encoder[T]): Encoder[Vector[T]] = iterableEncoder(encoder)

  implicit def mapEncoder[T](implicit valueencoderRes: Encoder[T]): UnresolvedEncoder[Map[String, T]] = {
    (env, update) =>
      val encoder = valueencoderRes(env, mapFullUpdate(extractMapValueSchema, update))

      new Encoder[Map[String, T]] {
        val schemaFor: SchemaFor[Map[String, T]] = mapSchemaFor(encoder.schemaFor)

        def encode(value: Map[String, T]): AnyRef = {
          val map = new util.HashMap[String, AnyRef]
          value.foreach { case (k, v) => map.put(k, encoder.encode(v)) }
          map
        }

        override def withSchema(schemaFor: SchemaFor[Map[String, T]]): Encoder[Map[String, T]] =
          buildWithSchema(mapEncoder(valueencoderRes), schemaFor)
      }
  }
}

trait CollectionAndContainerDecoders {

  import DecoderHelpers._

  implicit val NoneDecoder: Decoder[None.type] = new Decoder[None.type] {
    val schemaFor: SchemaFor[None.type] = noneSchemaFor
    def decode(value: Any): None.type =
      if (value == null) None else sys.error(s"Value $value is not null, but should be decoded to None")
  }

  implicit def optionDecoder[T](implicit valueDR: Decoder[T]): ResolvableDecoder[Option[T]] = { (env, update) =>
    val decoder = valueDR(env, mapFullUpdate(extractOptionSchema, update))

    new Decoder[Option[T]] {

      val schemaFor: SchemaFor[Option[T]] = optionSchemaFor(decoder.schemaFor)

      def decode(value: Any): Option[T] = if (value == null) None else Option(decoder.decode(value))

      override def withSchema(schemaFor: SchemaFor[Option[T]]): Decoder[Option[T]] =
        buildWithSchema(optionDecoder(valueDR), schemaFor)
    }
  }

  implicit def eitherDecoder[A: Manifest: WeakTypeTag, B: Manifest: WeakTypeTag](
      implicit leftDecoderRes: Decoder[A],
      rightDecoderRes: Decoder[B]): ResolvableDecoder[Either[A, B]] = { (env, update) =>
    val leftDecoder = leftDecoderRes(env, mapFullUpdate(extractEitherLeftSchema, update))
    val rightDecoder = rightDecoderRes(env, mapFullUpdate(extractEitherRightSchema, update))

    new Decoder[Either[A, B]] {
      val schemaFor: SchemaFor[Either[A, B]] = eitherSchemaFor(leftDecoder.schemaFor, rightDecoder.schemaFor)

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
        buildWithSchema(eitherDecoder(
                          manifest[A],
                          weakTypeTag[A],
                          manifest[B],
                          weakTypeTag[B],
                          leftDecoderRes,
                          rightDecoderRes
                        ),
                        schemaFor)
    }
  }

  implicit def arrayDecoder[T: ClassTag](implicit elemDecoderRes: Decoder[T]): ResolvableDecoder[Array[T]] = {
    (env, update) =>
      val decoder = elemDecoderRes(env, mapFullUpdate(extractIterableElementSchema, update))

      new Decoder[Array[T]] {
        val schemaFor: SchemaFor[Array[T]] = iterableSchemaFor(decoder.schemaFor).forType

        def decode(value: Any): Array[T] = value match {
          case array: Array[_]               => array.map(decoder.decode)
          case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toArray
          case list: Iterable[_]             => list.map(decoder.decode).toArray
          case other                         => sys.error("Unsupported array " + other)
        }

        override def withSchema(schemaFor: SchemaFor[Array[T]]): Decoder[Array[T]] =
          buildWithSchema(arrayDecoder(implicitly[ClassTag[T]], elemDecoderRes), schemaFor)
      }
  }

  private def iterableDecoder[T, C[X] <: Iterable[X]](elemDecoderRes: Decoder[T],
                                                      build: Iterable[T] => C[T]): ResolvableDecoder[C[T]] = {
    (env, update) =>
      val decoder = elemDecoderRes(env, mapFullUpdate(extractIterableElementSchema, update))

      new Decoder[C[T]] {
        val schemaFor: SchemaFor[C[T]] = iterableSchemaFor(decoder.schemaFor)

        def decode(value: Any): C[T] = value match {
          case list: java.util.Collection[_] => build(list.asScala.map(decoder.decode))
          case list: Iterable[_]             => build(list.map(decoder.decode))
          case array: Array[_]               =>
            // converting array to Seq in order to avoid requiring ClassTag[T] as does arrayDecoder.
            build(array.toSeq.map(decoder.decode))
          case other => sys.error("Unsupported array " + other)
        }

        override def withSchema(schemaFor: SchemaFor[C[T]]): Decoder[C[T]] =
          buildWithSchema(iterableDecoder(elemDecoderRes, build), schemaFor)
      }
  }

  implicit def listDecoder[T](implicit decoder: Decoder[T]): ResolvableDecoder[List[T]] =
    iterableDecoder(decoder, _.toList)
  implicit def mutableSeqDecoder[T](implicit decoder: Decoder[T]): ResolvableDecoder[scala.collection.mutable.Seq[T]] =
    iterableDecoder(decoder, _.toBuffer)
  implicit def seqDecoder[T](implicit decoder: Decoder[T]): ResolvableDecoder[Seq[T]] =
    iterableDecoder(decoder, _.toSeq)
  implicit def setDecoder[T](implicit decoder: Decoder[T]): ResolvableDecoder[Set[T]] =
    iterableDecoder(decoder, _.toSet)
  implicit def vectorDecoder[T](implicit decoder: Decoder[T]): ResolvableDecoder[Vector[T]] =
    iterableDecoder(decoder, _.toVector)

  implicit def mapDecoder[T](implicit valueDecoderRes: Decoder[T]): ResolvableDecoder[Map[String, T]] = {
    (env, update) =>
      val decoder = valueDecoderRes(env, mapFullUpdate(extractMapValueSchema, update))

      new Decoder[Map[String, T]] {
        val schemaFor: SchemaFor[Map[String, T]] = mapSchemaFor(decoder.schemaFor)

        def decode(value: Any): Map[String, T] = value match {
          case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> decoder.decode(v) }
        }

        override def withSchema(schemaFor: SchemaFor[Map[String, T]]): Decoder[Map[String, T]] =
          buildWithSchema(mapDecoder(valueDecoderRes), schemaFor)
      }
  }
}

object CollectionsAndContainers {

  val noneSchemaFor: SchemaFor[None.type] =
    SchemaFor(SchemaBuilder.builder.nullType)

  private[avro4s] def optionSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[Option[T]] =
    schemaFor.map[Option[T]](itemSchema => SchemaHelper.createSafeUnion(itemSchema, SchemaBuilder.builder().nullType()))

  private[avro4s] def extractOptionSchema(schema: Schema): Schema = {
    require(schema.getType == Schema.Type.UNION,
            s"Schema type for option encoders / decoders must be UNION, received ${schema.getType}")

    schema.getTypes.asScala.find(_.getType != Schema.Type.NULL) match {
      case Some(s) => s
      case None    => sys.error(s"Union schema $schema doesn't contain any non-null entries")
    }
  }

  private[avro4s] def eitherSchemaFor[A, B](leftSchemaFor: SchemaFor[A],
                                            rightSchemaFor: SchemaFor[B]): SchemaFor[Either[A, B]] =
    SchemaFor(SchemaHelper.createSafeUnion(leftSchemaFor.schema, rightSchemaFor.schema), leftSchemaFor.fieldMapper)

  private[avro4s] def extractEitherLeftSchema(schema: Schema): Schema = {
    validateEitherSchema(schema)
    schema.getTypes.get(0)
  }

  private[avro4s] def extractEitherRightSchema(schema: Schema): Schema = {
    validateEitherSchema(schema)
    schema.getTypes.get(1)
  }

  private[avro4s] def validateEitherSchema(schema: Schema): Unit = {
    require(schema.getType == Schema.Type.UNION,
            s"Schema type for either encoders / decoders must be UNION, received ${schema.getType}")
    require(schema.getTypes.size() == 2,
            s"Schema for either encoders / decoders must be a UNION of to types, received $schema")
  }

  private[avro4s] def iterableSchemaFor[C[X] <: Iterable[X], T](schemaFor: SchemaFor[T]): SchemaFor[C[T]] =
    schemaFor.map(SchemaBuilder.array.items(_))

  private[avro4s] def extractIterableElementSchema(schema: Schema): Schema = {
    require(
      schema.getType == Schema.Type.ARRAY,
      s"Schema type for array / list / seq / vector encoders and decoders must be ARRAY, received ${schema.getType}"
    )
    schema.getElementType
  }

  private[avro4s] def mapSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[Map[String, T]] =
    schemaFor.map(SchemaBuilder.map().values(_))

  private[avro4s] def extractMapValueSchema(schema: Schema): Schema = {
    require(schema.getType == Schema.Type.MAP,
            s"Schema type for map encoders / decoders must be MAP, received ${schema.getType}")
    schema.getValueType
  }
}
