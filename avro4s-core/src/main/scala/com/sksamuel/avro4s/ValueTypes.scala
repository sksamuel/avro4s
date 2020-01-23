package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
import com.sksamuel.avro4s.ValueTypes._
import magnolia.{CaseClass, Param}
import org.apache.avro.{Schema, SchemaBuilder}

class ValueTypeCodec[T](ctx: CaseClass[Codec, T], codec: ValueFieldCodec[T])
    extends Codec[T]
    with NamespaceAware[Codec[T]] {

  val schema = codec.schema

  def encode(value: T): AnyRef = codec.encodeValue(value)

  def decode(value: Any): T = decodeValueType(ctx, codec, value)

  def withNamespace(namespace: String): Codec[T] = ValueTypes.codec(ctx, NamespaceUpdate(namespace))

  override def withSchema(schemaFor: SchemaForV2[T]): Codec[T] = ValueTypes.codec(ctx, FullSchemaUpdate(schemaFor))
}

class ValueTypeEncoder[T](ctx: CaseClass[EncoderV2, T], encoder: ValueFieldEncoder[T])
    extends EncoderV2[T]
    with NamespaceAware[EncoderV2[T]] {

  val schema = encoder.schema

  def encode(value: T): AnyRef = encoder.encodeValue(value)

  def withNamespace(namespace: String): EncoderV2[T] = ValueTypes.encoder(ctx, NamespaceUpdate(namespace))

  override def withSchema(schemaFor: SchemaForV2[T]): EncoderV2[T] =
    ValueTypes.encoder(ctx, FullSchemaUpdate(schemaFor))
}

class ValueTypeDecoder[T](ctx: CaseClass[DecoderV2, T], decoder: ValueFieldDecoder[T])
    extends DecoderV2[T]
    with NamespaceAware[DecoderV2[T]] {

  val schema = decoder.schema

  def decode(value: Any): T = decodeValueType(ctx, decoder, value)

  def withNamespace(namespace: String): DecoderV2[T] = ValueTypes.decoder(ctx, NamespaceUpdate(namespace))

  override def withSchema(schemaFor: SchemaForV2[T]): DecoderV2[T] =
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

  def encoder[T](ctx: CaseClass[EncoderV2, T], schemaUpdate: SchemaUpdate): EncoderV2[T] =
    create(ctx, schemaUpdate, new EncoderBuilder)

  def decoder[T](ctx: CaseClass[DecoderV2, T], schemaUpdate: SchemaUpdate): DecoderV2[T] =
    create(ctx, schemaUpdate, new DecoderBuilder)

  private trait Builder[Typeclass[_], T, ValueProcessor[_]] {

    def processorConstructor: (Param[Typeclass, T], Schema) => ValueProcessor[T]

    def paramSchema: Param[Typeclass, T] => Schema

    def constructor: (CaseClass[Typeclass, T], ValueProcessor[T]) => Typeclass[T]
  }

  private class CodecBuilder[T] extends Builder[Codec, T, ValueFieldCodec] {
    val processorConstructor: (Param[Codec, T], Schema) => ValueFieldCodec[T] = new ValueFieldCodec(_, _)

    val paramSchema: Param[Codec, T] => Schema = _.typeclass.schema

    val constructor: (CaseClass[Codec, T], ValueFieldCodec[T]) => Codec[T] = new ValueTypeCodec(_, _)
  }

  private class DecoderBuilder[T] extends Builder[DecoderV2, T, ValueFieldDecoder] {
    val processorConstructor: (Param[DecoderV2, T], Schema) => ValueFieldDecoder[T] = new ValueFieldDecoder(_, _)

    val paramSchema: Param[DecoderV2, T] => Schema = _.typeclass.schema

    val constructor: (CaseClass[DecoderV2, T], ValueFieldDecoder[T]) => DecoderV2[T] = new ValueTypeDecoder(_, _)
  }

  private class EncoderBuilder[T] extends Builder[EncoderV2, T, ValueFieldEncoder] {
    val processorConstructor: (Param[EncoderV2, T], Schema) => ValueFieldEncoder[T] = new ValueFieldEncoder(_, _)

    val paramSchema: Param[EncoderV2, T] => Schema = _.typeclass.schema

    val constructor: (CaseClass[EncoderV2, T], ValueFieldEncoder[T]) => EncoderV2[T] = new ValueTypeEncoder(_ ,_)
  }

  private def create[Typeclass[_], T, ValueProcessor[_]](
      ctx: CaseClass[Typeclass, T],
      schemaUpdate: SchemaUpdate,
      builder: Builder[Typeclass, T, ValueProcessor]): Typeclass[T] = {
    val schema = schemaUpdate match {
      case FullSchemaUpdate(s) => s.schema
      case NamespaceUpdate(ns) => buildSchema(ctx, Some(ns), builder.paramSchema)
      case NoUpdate            => buildSchema(ctx, None, builder.paramSchema)
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

  class UnderlyingCodec[T](val param: Param[Codec, T], val schema: Schema) {

    // An encoder for a value type just needs to pass through the given value into an encoder
    // for the backing type. At runtime, the value type class won't exist, and the input
    // will be an instance of whatever the backing field of the value class was defined to be.
    // In other words, if you had a value type `case class Foo(str :String)` then the value
    // avro expects is a string, not a record of Foo, so the encoder for Foo should just encode
    // the underlying string

    private val codec: Codec[param.PType] = {
      val codec = param.typeclass
      codec match {
        case m: FieldSpecificCodec[param.PType] @unchecked if m.schema != schema =>
          m.forFieldWith(schema, param.annotations)
        case _ => codec
      }
    }

    def encodeUnderlying(value: T): AnyRef = codec.encode(param.dereference(value))

    // In Scala, value types are erased at compile time. So in avro4s we assume that value types do not exist
    // in the avro side, neither in the schema, nor in the input values. Therefore, when creating a decoder
    // for a value type, the input will be a value of the underlying type. In other words, given a value type
    // `Foo(s: String) extends AnyVal` - the input will be a String, not a record containing a String
    // as it would be for a non-value type.

    // So the generated decoder for a value type should simply pass through to a generated decoder for
    // the underlying type without worrying about fields etc.

    def decodeUnderlying(value: Any): param.PType = codec.decode(value)

  }

  class Base[Typeclass[X] <: SchemaAware[Typeclass, X], T](val param: Param[Typeclass, T], val schema: Schema) {

    // An encoder for a value type just needs to pass through the given value into an encoder
    // for the backing type. At runtime, the value type class won't exist, and the input
    // will be an instance of whatever the backing field of the value class was defined to be.
    // In other words, if you had a value type `case class Foo(str :String)` then the value
    // avro expects is a string, not a record of Foo, so the encoder for Foo should just encode
    // the underlying string

    protected val typeclass: Typeclass[param.PType] =
      if (param.typeclass.schema != schema) param.typeclass.withSchema(SchemaForV2(schema)) else param.typeclass

    @inline
    protected final def encode(encoder: EncoderV2[param.PType], value: T): AnyRef =
      encoder.encode(param.dereference(value))

    @inline
    protected final def decode(decoder: DecoderV2[param.PType], value: Any): param.PType = decoder.decode(value)
  }

  trait ValueDecoder {
    def decodeValue(value: Any): Any
  }

  class ValueFieldEncoder[T](p: Param[EncoderV2, T], schema: Schema) extends Base(p, schema) {
    def encodeValue(value: T): AnyRef = encode(typeclass, value)
  }

  class ValueFieldDecoder[T](param: Param[DecoderV2, T], schema: Schema) extends Base(param, schema) with ValueDecoder {
    def decodeValue(value: Any): Any = decode(typeclass, value)
  }

  class ValueFieldCodec[T](param: Param[Codec, T], schema: Schema) extends Base(param, schema) with ValueDecoder {

    def encodeValue(value: T): AnyRef = encode(typeclass, value)

    def decodeValue(value: Any): Any = decode(typeclass, value)
  }
}
