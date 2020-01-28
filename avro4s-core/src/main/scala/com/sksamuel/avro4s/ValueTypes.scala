package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, UseFieldMapper}
import com.sksamuel.avro4s.ValueTypes._
import magnolia.{CaseClass, Param}
import org.apache.avro.{Schema, SchemaBuilder}

class ValueTypeCodec[T](ctx: CaseClass[Codec, T], codec: ValueFieldCodec[T])
    extends Codec[T]
    with NamespaceAware[Codec[T]] {

  val schemaFor = codec.schemaFor

  def encode(value: T): AnyRef = codec.encodeValue(value)

  def decode(value: Any): T = decodeValueType(ctx, codec, value)

  def withNamespace(namespace: String): Codec[T] =
    ValueTypes.codec(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaForV2[T]): Codec[T] = ValueTypes.codec(ctx, FullSchemaUpdate(schemaFor))
}

class ValueTypeEncoder[T](ctx: CaseClass[Encoder, T], encoder: ValueFieldEncoder[T])
    extends Encoder[T]
    with NamespaceAware[Encoder[T]] {

  val schemaFor = encoder.schemaFor

  def encode(value: T): AnyRef = encoder.encodeValue(value)

  def withNamespace(namespace: String): Encoder[T] =
    ValueTypes.encoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaForV2[T]): Encoder[T] =
    ValueTypes.encoder(ctx, FullSchemaUpdate(schemaFor))
}

class ValueTypeDecoder[T](ctx: CaseClass[Decoder, T], decoder: ValueFieldDecoder[T])
    extends Decoder[T]
    with NamespaceAware[Decoder[T]] {

  val schemaFor = decoder.schemaFor

  def decode(value: Any): T = decodeValueType(ctx, decoder, value)

  def withNamespace(namespace: String): Decoder[T] =
    ValueTypes.decoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaForV2[T]): Decoder[T] =
    ValueTypes.decoder(ctx, FullSchemaUpdate(schemaFor))
}

object ValueTypes {

  @inline
  private[avro4s] def decodeValueType[Typeclass[_], T](ctx: CaseClass[Typeclass, T],
                                                       decoder: ValueDecoder,
                                                       value: Any): T =
    ctx.rawConstruct(List(decoder.decodeValue(value)))

  def codec[T](ctx: CaseClass[Codec, T], schemaUpdate: SchemaUpdate): Codec[T] =
    create(ctx, schemaUpdate, new CodecBuilder)

  def encoder[T](ctx: CaseClass[Encoder, T], schemaUpdate: SchemaUpdate): Encoder[T] =
    create(ctx, schemaUpdate, new EncoderBuilder)

  def decoder[T](ctx: CaseClass[Decoder, T], schemaUpdate: SchemaUpdate): Decoder[T] =
    create(ctx, schemaUpdate, new DecoderBuilder)

  private[avro4s] trait Builder[Typeclass[_], T, ValueProcessor[_]] {

    def processorConstructor: (Param[Typeclass, T], SchemaForV2[T]) => ValueProcessor[T]

    def paramSchema: Param[Typeclass, T] => Schema

    def constructor: (CaseClass[Typeclass, T], ValueProcessor[T]) => Typeclass[T]
  }

  private[avro4s] class CodecBuilder[T] extends Builder[Codec, T, ValueFieldCodec] {
    val processorConstructor = new ValueFieldCodec(_, _)

    val paramSchema = _.typeclass.schemaFor.schema

    val constructor: (CaseClass[Codec, T], ValueFieldCodec[T]) => Codec[T] = new ValueTypeCodec(_, _)
  }

  private[avro4s] class DecoderBuilder[T] extends Builder[Decoder, T, ValueFieldDecoder] {
    val processorConstructor = new ValueFieldDecoder[T](_, _)

    val paramSchema = _.typeclass.schemaFor.schema

    val constructor = new ValueTypeDecoder(_, _)
  }

  private[avro4s] class EncoderBuilder[T] extends Builder[Encoder, T, ValueFieldEncoder] {
    val processorConstructor = new ValueFieldEncoder(_, _)

    val paramSchema = _.typeclass.schemaFor.schema

    val constructor: (CaseClass[Encoder, T], ValueFieldEncoder[T]) => Encoder[T] = new ValueTypeEncoder(_, _)
  }

  private def create[Typeclass[_], T, ValueProcessor[_]](
      ctx: CaseClass[Typeclass, T],
      schemaUpdate: SchemaUpdate,
      builder: Builder[Typeclass, T, ValueProcessor]): Typeclass[T] = {
    val schema = schemaUpdate match {
      case FullSchemaUpdate(s)    => s.forType[T]
      case NamespaceUpdate(ns, _) => SchemaForV2(buildSchema(ctx, Some(ns), builder.paramSchema)).forType[T]
      case UseFieldMapper(_)      => SchemaForV2(buildSchema(ctx, None, builder.paramSchema)).forType[T]
    }
    val processor = builder.processorConstructor(ctx.parameters.head, schema)
    builder.constructor(ctx, processor)
  }

  def buildSchema[Typeclass[_], T](ctx: CaseClass[Typeclass, T],
                                   namespaceUpdate: Option[String],
                                   paramSchema: Param[Typeclass, T] => Schema): Schema = {
    val annotationExtractor = new AnnotationExtractors(ctx.annotations) // taking over @AvroFixed and the like

    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
    val namespace = namespaceUpdate.getOrElse(nameExtractor.namespace)
    val name = nameExtractor.name

    // if the class is a value type, then we need to use the schema for the single field inside the type
    // in other words, if we have `case class Foo(str:String)` then this just acts like a string
    // if we have a value type AND @AvroFixed is present on the class, then we simply return a schema of type fixed

    val param = ctx.parameters.head
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
      case None => paramSchema(param)
    }
  }

  private[avro4s] class Base[Typeclass[X] <: SchemaAware[Typeclass, X], T](val param: Param[Typeclass, T],
                                                                           val schemaFor: SchemaForV2[T]) {

    // An encoder for a value type just needs to pass through the given value into an encoder
    // for the backing type. At runtime, the value type class won't exist, and the input
    // will be an instance of whatever the backing field of the value class was defined to be.
    // In other words, if you had a value type `case class Foo(str :String)` then the value
    // avro expects is a string, not a record of Foo, so the encoder for Foo should just encode
    // the underlying string

    protected val typeclass: Typeclass[param.PType] =
      if (param.typeclass.schema != schemaFor.schema) param.typeclass.withSchema(schemaFor.forType) else param.typeclass

    @inline
    protected final def encode(encoder: Encoder[param.PType], value: T): AnyRef =
      encoder.encode(param.dereference(value))

    @inline
    protected final def decode(decoder: Decoder[param.PType], value: Any): param.PType = decoder.decode(value)
  }

  private[avro4s] trait ValueDecoder {
    def decodeValue(value: Any): Any
  }

  private[avro4s] class ValueFieldEncoder[T](p: Param[Encoder, T], schemaFor: SchemaForV2[T])
      extends Base(p, schemaFor) {
    def encodeValue(value: T): AnyRef = encode(typeclass, value)
  }

  private[avro4s] class ValueFieldDecoder[T](param: Param[Decoder, T], schemaFor: SchemaForV2[T])
      extends Base(param, schemaFor)
      with ValueDecoder {
    def decodeValue(value: Any): Any = decode(typeclass, value)
  }

  private[avro4s] class ValueFieldCodec[T](param: Param[Codec, T], schemaFor: SchemaForV2[T])
      extends Base(param, schemaFor)
      with ValueDecoder {

    def encodeValue(value: T): AnyRef = encode(typeclass, value)

    def decodeValue(value: Any): Any = decode(typeclass, value)
  }
}
