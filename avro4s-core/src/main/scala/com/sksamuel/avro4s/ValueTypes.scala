package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
import com.sksamuel.avro4s.ValueTypes._
import magnolia.{CaseClass, Param}
import org.apache.avro.{Schema, SchemaBuilder}
import scala.reflect.runtime.universe._

class ValueTypeEncoder[T: WeakTypeTag](ctx: CaseClass[Encoder, T]) extends Encoder[T] {

  private[avro4s] var encoder: FieldEncoder[T]#ValueEncoder = _

  def schemaFor = encoder.schemaFor

  def encode(value: T): AnyRef = encoder.encodeValue(value)

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] =
    ValueTypes.encoder(ctx, DefinitionEnvironment.empty, FullSchemaUpdate(schemaFor))
}

class ValueTypeDecoder[T: WeakTypeTag](ctx: CaseClass[Decoder, T]) extends Decoder[T] {

  private[avro4s] var decoder: FieldDecoder[T]#ValueDecoder = _

  def schemaFor = decoder.schemaFor

  def decode(value: Any): T = ctx.rawConstruct(List(decoder.decodeValue(value)))

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] =
    ValueTypes.decoder(ctx, DefinitionEnvironment.empty, FullSchemaUpdate(schemaFor))
}

object ValueTypes {

  def encoder[T: WeakTypeTag](ctx: CaseClass[Encoder, T],
                              env: DefinitionEnvironment[Encoder],
                              update: SchemaUpdate): Encoder[T] = {
    val encoder = new ValueTypeEncoder[T](ctx)
    val nextEnv = env.updated(encoder)
    encoder.encoder = new FieldEncoder[T](ctx.parameters.head)(ctx, nextEnv, update)
    encoder
  }

  def decoder[T: WeakTypeTag](ctx: CaseClass[Decoder, T],
                              env: DefinitionEnvironment[Decoder],
                              update: SchemaUpdate): Decoder[T] = {
    val decoder = new ValueTypeDecoder[T](ctx)
    val nextEnv = env.updated(decoder)
    decoder.decoder = new FieldDecoder[T](ctx.parameters.head)(ctx, nextEnv, update)
    decoder
  }

  def schema[T](ctx: CaseClass[SchemaFor, T],
                env: DefinitionEnvironment[SchemaFor],
                update: SchemaUpdate): SchemaFor[T] =
    buildSchemaFor(ctx, ctx.parameters.head.typeclass.resolveSchemaFor(env, update).schema, update)

  private def buildSchemaFor[Typeclass[_], T](ctx: CaseClass[Typeclass, T],
                                              paramSchema: Schema,
                                              update: SchemaUpdate): SchemaFor[T] = {
    def buildSchema(namespaceUpdate: Option[String]): Schema = {
      val annotationExtractor = new AnnotationExtractors(ctx.annotations) // taking over @AvroFixed and the like

      val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
      val namespace = namespaceUpdate.getOrElse(nameExtractor.namespace)
      val name = nameExtractor.name

      // if the class is a value type, then we need to use the schema for the single field inside the type
      // in other words, if we have `case class Foo(str:String)` then this just acts like a string
      // if we have a value type AND @AvroFixed is present on the class, then we simply return a schema of type fixed

      annotationExtractor.fixed match {
        case Some(size) =>
          val builder =
            SchemaBuilder
              .fixed(name)
              .doc(annotationExtractor.doc.orNull)
              .namespace(namespace)
              .aliases(annotationExtractor.aliases: _*)
          annotationExtractor.props.foreach { case (k, v) => builder.prop(k, v) }
          builder.size(size)
        case None => paramSchema
      }
    }
    update match {
      case FullSchemaUpdate(s) => s.forType[T]
      case NamespaceUpdate(ns) => SchemaFor(buildSchema(Some(ns)), DefaultFieldMapper).forType[T]
      case NoUpdate            => SchemaFor(buildSchema(None), DefaultFieldMapper).forType[T]
    }
  }

  private def fieldUpdate[Typeclass[_], T](ctx: CaseClass[Typeclass, T]): SchemaUpdate = {
    val annotationExtractor = new AnnotationExtractors(ctx.annotations)
    (annotationExtractor.fixed, annotationExtractor.namespace) match {
      case (Some(size), namespace) =>
        val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
        val name = nameExtractor.name
        val ns = namespace.getOrElse(nameExtractor.namespace)
        FullSchemaUpdate(SchemaFor(SchemaBuilder.fixed(name).namespace(ns).size(size)))
      case (_, Some(ns)) => NamespaceUpdate(ns)
      case _             => NoUpdate
    }
  }

  class FieldEncoder[T](p: Param[Encoder, T]) {
    // An encoder for a value type just needs to pass through the given value into an encoder
    // for the backing type. At runtime, the value type class won't exist, and the input
    // will be an instance of whatever the backing field of the value class was defined to be.
    // In other words, if you had a value type `case class Foo(str :String)` then the value
    // avro expects is a string, not a record of Foo, so the encoder for Foo should just encode
    // the underlying string

    class ValueEncoder(encoder: Encoder[p.PType], val schemaFor: SchemaFor[T]) {
      def encodeValue(value: T): AnyRef = encoder.encode(p.dereference(value))
    }

    def apply(ctx: CaseClass[Encoder, T], env: DefinitionEnvironment[Encoder], update: SchemaUpdate) = {
      val encoder = p.typeclass.resolveEncoder(env, fieldUpdate(ctx))
      val schemaFor = buildSchemaFor(ctx, encoder.schemaFor.schema, update)
      new ValueEncoder(encoder, schemaFor)
    }
  }

  class FieldDecoder[T](p: Param[Decoder, T]) {
    class ValueDecoder(decoder: Decoder[p.PType], val schemaFor: SchemaFor[T]) {
      def decodeValue(value: Any): Any = decoder.decode(value)
    }

    def apply(ctx: CaseClass[Decoder, T], env: DefinitionEnvironment[Decoder], update: SchemaUpdate) = {
      val decoder = p.typeclass.resolveDecoder(env, fieldUpdate(ctx))
      val schemaFor = buildSchemaFor(ctx, decoder.schemaFor.schema, update)
      new ValueDecoder(decoder, schemaFor)
    }
  }
}
