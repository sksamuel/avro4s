package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, UseFieldMapper}
import com.sksamuel.avro4s.ValueTypes._
import magnolia.{CaseClass, Param}
import org.apache.avro.{Schema, SchemaBuilder}

class ValueTypeEncoder[T](ctx: CaseClass[Encoder, T], encoder: ValueFieldEncoder[T])
    extends Encoder[T]
    with NamespaceAware[Encoder[T]] {

  val schemaFor = encoder.schemaFor

  def encode(value: T): AnyRef = encoder.encodeValue(value)

  def withNamespace(namespace: String): Encoder[T] =
    ValueTypes.encoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] =
    ValueTypes.encoder(ctx, FullSchemaUpdate(schemaFor))
}

class ValueTypeDecoder[T](ctx: CaseClass[Decoder, T], decoder: ValueFieldDecoder[T])
    extends Decoder[T]
    with NamespaceAware[Decoder[T]] {

  val schemaFor = decoder.schemaFor

  def decode(value: Any): T = ctx.rawConstruct(List(decoder.decodeValue(value)))

  def withNamespace(namespace: String): Decoder[T] =
    ValueTypes.decoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] =
    ValueTypes.decoder(ctx, FullSchemaUpdate(schemaFor))
}

object ValueTypes {

  def encoder[T](ctx: CaseClass[Encoder, T], schemaUpdate: SchemaUpdate): Encoder[T] = {
    val param = ctx.parameters.head
    val schemaFor = buildSchemaFor(ctx, param.typeclass.schema, schemaUpdate)
    new ValueTypeEncoder[T](ctx, new ValueFieldEncoder[T](param, schemaFor))
  }

  def decoder[T](ctx: CaseClass[Decoder, T], schemaUpdate: SchemaUpdate): Decoder[T] = {
    val param = ctx.parameters.head
    val schemaFor = buildSchemaFor(ctx, param.typeclass.schema, schemaUpdate)
    new ValueTypeDecoder[T](ctx, new ValueFieldDecoder[T](param, schemaFor))
  }

  def schema[T](ctx: CaseClass[SchemaFor, T], fieldMapper: FieldMapper): SchemaFor[T] =
    SchemaFor[T](buildSchema(ctx, None, ctx.parameters.head.typeclass.schema), fieldMapper)

  private def buildSchemaFor[Typeclass[_], T](ctx: CaseClass[Typeclass, T], paramSchema: Schema, schemaUpdate: SchemaUpdate): SchemaFor[T] =
    schemaUpdate match {
      case FullSchemaUpdate(s)    => s.forType[T]
      case NamespaceUpdate(ns, _) => SchemaFor(buildSchema(ctx, Some(ns), paramSchema)).forType[T]
      case UseFieldMapper(_)      => SchemaFor(buildSchema(ctx, None, paramSchema)).forType[T]
    }

  private def buildSchema[Typeclass[_], T](ctx: CaseClass[Typeclass, T],
                                   namespaceUpdate: Option[String],
                                   paramSchema: Schema): Schema = {
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

  private[avro4s] class Base[Typeclass[X] <: SchemaAware[Typeclass, X], T](val param: Param[Typeclass, T],
                                                                           val schemaFor: SchemaFor[T]) extends Serializable {

    // An encoder for a value type just needs to pass through the given value into an encoder
    // for the backing type. At runtime, the value type class won't exist, and the input
    // will be an instance of whatever the backing field of the value class was defined to be.
    // In other words, if you had a value type `case class Foo(str :String)` then the value
    // avro expects is a string, not a record of Foo, so the encoder for Foo should just encode
    // the underlying string

    protected val typeclass: Typeclass[param.PType] =
      if (param.typeclass.schema != schemaFor.schema) param.typeclass.withSchema(schemaFor.forType) else param.typeclass

  }

  private[avro4s] class ValueFieldEncoder[T](p: Param[Encoder, T], schemaFor: SchemaFor[T])
      extends Base(p, schemaFor) {
    def encodeValue(value: T): AnyRef = typeclass.encode(param.dereference(value))
  }

  private[avro4s] class ValueFieldDecoder[T](param: Param[Decoder, T], schemaFor: SchemaFor[T])
      extends Base(param, schemaFor) {
    def decodeValue(value: Any): Any = typeclass.decode(value)
  }
}
