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

      private implicit val elementDecoder: PartialFunction[Any, H] = TypeGuardedDecoding.guard(codecH)

      def decode(value: Any): H :+: T = decodeCoproduct[H, T](value)

      override def withSchema(schemaFor: SchemaForV2[H :+: T]): Codec[H :+: T] =
        coproductCodec[H, T](implicitly[WeakTypeTag[H]],
                             implicitly[Manifest[H]],
                             withSelectedSubSchema(codecH, schemaFor),
                             withFullSchema(codecT, schemaFor))
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

      override def withSchema(schemaFor: SchemaForV2[H :+: T]): EncoderV2[H :+: T] =
        coproductEncoder[H, T](
          implicitly[WeakTypeTag[H]],
          implicitly[Manifest[H]],
          withSelectedSubSchema(encoderH, schemaFor),
          withFullSchema(encoderT, schemaFor)
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

      private implicit val elementDecoder: PartialFunction[Any, H] = TypeGuardedDecoding.guard(decoderH)

      def decode(value: Any): H :+: T = decodeCoproduct[H, T](value)

      override def withSchema(schemaFor: SchemaForV2[H :+: T]): DecoderV2[H :+: T] =
        coproductDecoder[H, T](
          implicitly[WeakTypeTag[H]],
          implicitly[Manifest[H]],
          withSelectedSubSchema(decoderH, schemaFor),
          withFullSchema(decoderT, schemaFor)
        )
    }
}

object ShapelessCoproducts {

  object CNilCodec extends Codec[CNil] {
    val schema: Schema = Schema.createUnion(Collections.emptyList[Schema]())

    def encode(value: CNil): AnyRef = sys.error(s"Unexpected value '$value' of type CNil (that doesn't exist)")

    def decode(value: Any): CNil = sys.error(s"Unable to decode value '$value'")

    override def withSchema(schemaFor: SchemaForV2[CNil]): Codec[CNil] = this
  }

  private[avro4s] def coproductSchema[A[_], B[_]](aware1: SchemaAware[A, _], aware2: SchemaAware[B, _]): Schema =
    SchemaHelper.createSafeUnion(aware1.schema, aware2.schema)

  private[avro4s] def withSelectedSubSchema[T[_], V](aware: SchemaAware[T, V], schemaFor: SchemaForV2[_])(
      implicit m: Manifest[V]): T[V] = {
    val schema = schemaFor.schema
    val fullName = NameExtractor(manifest.runtimeClass).fullName
    val subSchemaFor = SchemaForV2[V](SchemaHelper.extractTraitSubschema(fullName, schema))
    aware.withSchema(subSchemaFor)
  }

  def withFullSchema[T[_], V](aware: SchemaAware[T, V], schemaFor: SchemaForV2[_]): T[V] =
    aware.withSchema(schemaFor.map(identity))

  private[avro4s] def encodeCoproduct[H, T <: Coproduct](value: H :+: T)(implicit encoderH: EncoderV2[H],
                                                                         encoderT: EncoderV2[T]): AnyRef =
    value match {
      case Inl(h) => encoderH.encode(h)
      case Inr(t) => encoderT.encode(t)
    }

  private[avro4s] def decodeCoproduct[H, T <: Coproduct](value: Any)(implicit elementDecoder: PartialFunction[Any, H],
                                                                     decoderT: DecoderV2[T]): H :+: T =
    if (elementDecoder.isDefinedAt(value)) Inl(elementDecoder(value)) else Inr(decoderT.decode(value))
}
