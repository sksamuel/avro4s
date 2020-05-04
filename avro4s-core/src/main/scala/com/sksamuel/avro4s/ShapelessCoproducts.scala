package com.sksamuel.avro4s

import java.util.Collections

import com.sksamuel.avro4s.ShapelessCoproducts._
import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct, Inl, Inr}

import scala.reflect.runtime.universe._

trait ShapelessCoproductSchemaFors {

  implicit def singleElementSchemaFor[H](implicit h: SchemaFor[H]): SchemaFor[H :+: CNil] =
    new ResolvableSchemaFor[H :+: CNil] {
      def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[H :+: CNil] =
        buildSingleElementSchemaFor(h.resolveSchemaFor(env, update))
    }

  implicit def coproductSchemaFor[H, T <: Coproduct](implicit h: SchemaFor[H], t: SchemaFor[T]): SchemaFor[H :+: T] =
    new ResolvableSchemaFor[H :+: T] {
      def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[H :+: T] =
        buildCoproductSchemaFor(h.resolveSchemaFor(env, update), t.resolveSchemaFor(env, update))
    }
}

trait ShapelessCoproductEncoders {
  import com.sksamuel.avro4s.EncoderHelpers.{buildWithSchema, mapFullUpdate}

  implicit val CNilEncoder: Encoder[CNil] = new Encoder[CNil] {
    val schemaFor: SchemaFor[CNil] = CNilSchemaFor
    def encode(value: CNil): AnyRef = sys.error(s"Unexpected value '$value' of type CNil (that doesn't exist)")
    override def withSchema(schemaFor: SchemaFor[CNil]): Encoder[CNil] = this
  }

  implicit def coproductEncoder[H: WeakTypeTag, T <: Coproduct](implicit h: Encoder[H],
                                                                t: Encoder[T]): Encoder[H :+: T] =
    new ResolvableEncoder[H :+: T] {
      def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[H :+: T] = {
        val encoderH = h.resolveEncoder(env, mapFullUpdate(takeFirstType, update))
        val encoderT = t.resolveEncoder(env, mapFullUpdate(dropFirstType, update))
        new Encoder[H :+: T] {

          val schemaFor: SchemaFor[H :+: T] = buildCoproductSchemaFor(encoderH.schemaFor, encoderT.schemaFor)

          def encode(value: H :+: T): AnyRef = value match {
            case Inl(h) => encoderH.encode(h)
            case Inr(t) => encoderT.encode(t)
          }

          override def withSchema(schemaFor: SchemaFor[H :+: T]): Encoder[H :+: T] =
            buildWithSchema(coproductEncoder(implicitly[WeakTypeTag[H]], h, t), schemaFor)
        }
      }
    }
}

trait ShapelessCoproductDecoders {
  import com.sksamuel.avro4s.DecoderHelpers.{buildWithSchema, mapFullUpdate}

  implicit val CNilDecoder: Decoder[CNil] = new Decoder[CNil] {
    val schemaFor: SchemaFor[CNil] = CNilSchemaFor

    def decode(value: Any): CNil = sys.error(s"Unable to decode value '$value'")

    override def withSchema(schemaFor: SchemaFor[CNil]): Decoder[CNil] = this
  }

  implicit final def coproductDecoder[H: WeakTypeTag, T <: Coproduct](implicit h: Decoder[H],
                                                                      t: Decoder[T]): Decoder[H :+: T] =
    new ResolvableDecoder[H :+: T] {
      def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[H :+: T] = {
        val decoderH = h.resolveDecoder(env, mapFullUpdate(takeFirstType, update))
        val decoderT = t.resolveDecoder(env, mapFullUpdate(dropFirstType, update))
        new Decoder[H :+: T] {

          val schemaFor: SchemaFor[H :+: T] = buildCoproductSchemaFor(decoderH.schemaFor, decoderT.schemaFor)

          private val elementDecoder: PartialFunction[Any, H] = TypeGuardedDecoding.guard(decoderH)

          def decode(value: Any): H :+: T =
            if (elementDecoder.isDefinedAt(value)) Inl(elementDecoder(value)) else Inr(decoderT.decode(value))

          override def withSchema(schemaFor: SchemaFor[H :+: T]): Decoder[H :+: T] =
            buildWithSchema(coproductDecoder[H, T](implicitly[WeakTypeTag[H]], h, t), schemaFor)
        }
      }
    }
}

object ShapelessCoproducts {

  private[avro4s] val CNilSchemaFor: SchemaFor[CNil] =
    SchemaFor(Schema.createUnion(Collections.emptyList[Schema]()), DefaultFieldMapper)

  private[avro4s] def buildSingleElementSchemaFor[H](schemaForH: SchemaFor[H]): SchemaFor[H :+: CNil] =
    SchemaFor(SchemaHelper.createSafeUnion(schemaForH.schema), schemaForH.fieldMapper)

  private[avro4s] def buildCoproductSchemaFor[H, T <: Coproduct](schemaForH: SchemaFor[H],
                                                                 schemaForT: SchemaFor[T]): SchemaFor[H :+: T] =
    SchemaFor(SchemaHelper.createSafeUnion(schemaForH.schema, schemaForT.schema), schemaForH.fieldMapper)

  private[avro4s] def takeFirstType(schema: Schema): Schema = schema.getTypes.get(0)

  private[avro4s] def dropFirstType(schema: Schema): Schema = {
    import scala.collection.JavaConverters._
    Schema.createUnion(schema.getTypes.asScala.drop(1).asJava)
  }
}
