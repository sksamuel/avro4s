package com.sksamuel.avro4s

import java.util.Collections

import com.sksamuel.avro4s.ShapelessCoproducts._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData}
import org.apache.avro.util.Utf8
import shapeless.{:+:, CNil, Coproduct, Inl, Inr}

import scala.reflect.runtime.universe._

trait ShapelessCoproductCodecs {

  implicit final val CNilCodec: Codec[CNil] = ShapelessCoproducts.CNilCodec

  implicit final def coproductCodec[H: WeakTypeTag: Manifest, T <: Coproduct](implicit codecH: Codec[H],
                                                                              codecT: Codec[T]): Codec[H :+: T] =
    new Codec[H :+: T] {

      val schema: Schema = coproductSchema(codecH, codecT)

      def encode(value: H :+: T): AnyRef = encodeCoproduct[H, T](value)

      private implicit val elementDecoder: PartialFunction[Any, H] = buildElementDecoder(codecH)

      def decode(value: Any): H :+: T = decodeCoproduct[H, T](value)

      override def withSchema(schemaFor: SchemaForV2[H :+: T], fieldMapper: FieldMapper): Codec[H :+: T] =
        coproductCodec[H, T](implicitly[WeakTypeTag[H]],
                             implicitly[Manifest[H]],
                             withSelectedSubSchema(codecH, schemaFor, fieldMapper),
                             withFullSchema(codecT, schemaFor, fieldMapper))
    }
}

trait ShapelessCoproductEncoders {

  implicit final val CNilEncoder: EncoderV2[CNil] = ShapelessCoproducts.CNilCodec

  implicit final def coproductEncoder[H: WeakTypeTag: Manifest, T <: Coproduct](
      implicit encoderH: EncoderV2[H],
      encoderT: EncoderV2[T]): EncoderV2[H :+: T] =
    new EncoderV2[H :+: T] {

      val schema: Schema = coproductSchema(encoderH, encoderT)

      def encode(value: H :+: T): AnyRef = encodeCoproduct[H, T](value)

      override def withSchema(schemaFor: SchemaForV2[H :+: T], fieldMapper: FieldMapper): EncoderV2[H :+: T] =
        coproductEncoder[H, T](
          implicitly[WeakTypeTag[H]],
          implicitly[Manifest[H]],
          withSelectedSubSchema(encoderH, schemaFor, fieldMapper),
          withFullSchema(encoderT, schemaFor, fieldMapper)
        )
    }
}

trait ShapelessCoproductDecoders {

  implicit val CNilDecoder: DecoderV2[CNil] = ShapelessCoproducts.CNilCodec

  implicit final def coproductDecoder[H: WeakTypeTag: Manifest, T <: Coproduct](
      implicit decoderH: DecoderV2[H],
      decoderT: DecoderV2[T]): DecoderV2[H :+: T] =
    new DecoderV2[H :+: T] {

      val schema: Schema = coproductSchema(decoderH, decoderT)

      private implicit val elementDecoder: PartialFunction[Any, H] = buildElementDecoder(decoderH)

      def decode(value: Any): H :+: T = decodeCoproduct[H, T](value)

      override def withSchema(schemaFor: SchemaForV2[H :+: T], fieldMapper: FieldMapper): DecoderV2[H :+: T] =
        coproductDecoder[H, T](
          implicitly[WeakTypeTag[H]],
          implicitly[Manifest[H]],
          withSelectedSubSchema(decoderH, schemaFor, fieldMapper),
          withFullSchema(decoderT, schemaFor, fieldMapper)
        )
    }
}

object ShapelessCoproducts {

  object CNilCodec extends Codec[CNil] {
    val schema: Schema = Schema.createUnion(Collections.emptyList[Schema]())

    def encode(value: CNil): AnyRef = sys.error(s"Unexpected value '$value' of type CNil (that doesn't exist)")

    def decode(value: Any): CNil = sys.error(s"Unable to decode value '$value'")

    override def withSchema(schemaFor: SchemaForV2[CNil], fieldMapper: FieldMapper): Codec[CNil] = this
  }

  private[avro4s] def coproductSchema[A[_], B[_]](aware1: SchemaAware[A, _], aware2: SchemaAware[B, _]): Schema =
    SchemaHelper.createSafeUnion(aware1.schema, aware2.schema)

  private[avro4s] def withSelectedSubSchema[T[_], V](aware: SchemaAware[T, V],
                                                     schemaFor: SchemaForV2[_],
                                                     fieldMapper: FieldMapper)(implicit m: Manifest[V]): T[V] = {
    val schema = schemaFor.schema
    val fullName = NameExtractor(manifest.runtimeClass).fullName
    val subSchemaFor = SchemaForV2[V](SchemaHelper.extractTraitSubschema(fullName, schema))
    aware.withSchema(subSchemaFor, fieldMapper)
  }

  def withFullSchema[T[_], V](aware: SchemaAware[T, V], schemaFor: SchemaForV2[_], fieldMapper: FieldMapper): T[V] =
    aware.withSchema(schemaFor.map(identity), fieldMapper)

  private[avro4s] def encodeCoproduct[H, T <: Coproduct](value: H :+: T)(implicit encoderH: EncoderV2[H],
                                                                         encoderT: EncoderV2[T]): AnyRef =
    value match {
      case Inl(h) => encoderH.encode(h)
      case Inr(t) => encoderT.encode(t)
    }

  private[avro4s] def decodeCoproduct[H, T <: Coproduct](value: Any)(implicit elementDecoder: PartialFunction[Any, H],
                                                                     decoderT: DecoderV2[T]): H :+: T =
    if (elementDecoder.isDefinedAt(value)) Inl(elementDecoder(value)) else Inr(decoderT.decode(value))

  private[avro4s] def buildElementDecoder[T: WeakTypeTag: Manifest](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) stringDecoder(decoder)
    else if (tpe <:< typeOf[Boolean]) booleanDecoder(decoder)
    else if (tpe <:< typeOf[Int]) intDecoder(decoder)
    else if (tpe <:< typeOf[Long]) longDecoder(decoder)
    else if (tpe <:< typeOf[Double]) doubleDecoder(decoder)
    else if (tpe <:< typeOf[Float]) floatDecoder(decoder)
    else if (tpe <:< typeOf[Array[_]] || tpe <:< typeOf[java.util.Collection[_]] || tpe <:< typeOf[Iterable[_]]) {
      arrayDecoder(decoder)
    } else if (tpe <:< typeOf[java.util.Map[_, _]] || tpe <:< typeOf[Map[_, _]]) {
      mapDecoder(decoder)
    } else {
      val nameExtractor = NameExtractor(manifest.runtimeClass)
      recordDecoder(nameExtractor.fullName, decoder)
    }
  }

  def stringDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Utf8   => decoder.decode(v)
    case v: String => decoder.decode(v)
  }

  def booleanDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Boolean => decoder.decode(v)
  }

  def intDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Int => decoder.decode(v)
  }

  def longDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Long => decoder.decode(v)
  }

  def doubleDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Double => decoder.decode(v)
  }

  def floatDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: Float => decoder.decode(v)
  }

  def arrayDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: GenericData.Array[_] => decoder.decode(v)
  }

  def mapDecoder[T](decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: java.util.Map[_, _] => decoder.decode(v)
  }

  def recordDecoder[T](typeName: String, decoder: DecoderV2[T]): PartialFunction[Any, T] = {
    case v: GenericContainer if v.getSchema.getFullName == typeName => decoder.decode(v)
  }
}
