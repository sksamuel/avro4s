package com.sksamuel.avro4s

import com.sksamuel.avro4s.RecordFields._
import com.sksamuel.avro4s.Records._
import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
import magnolia.{CaseClass, Param}
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{JsonProperties, Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

class RecordEncoder[T](ctx: CaseClass[EncoderV2, T], val schemaFor: SchemaForV2[T], fieldEncoding: Seq[FieldEncoder[T]])
    extends EncoderV2[T]
    with NamespaceAware[EncoderV2[T]] {

  def encode(value: T): AnyRef = encodeRecord(ctx, schema, fieldEncoding, value)

  def withNamespace(namespace: String): EncoderV2[T] = encoder(ctx, schemaFor.fieldMapper, NamespaceUpdate(namespace))

  override def withSchema(schemaFor: SchemaForV2[T]): EncoderV2[T] = {
    verifyNewSchema(schemaFor)
    encoder(ctx, schemaFor.fieldMapper, FullSchemaUpdate(schemaFor))
  }
}

class RecordDecoder[T](ctx: CaseClass[DecoderV2, T], val schemaFor: SchemaForV2[T], fieldDecoding: Seq[FieldDecoder])
    extends DecoderV2[T]
    with NamespaceAware[DecoderV2[T]] {

  def decode(value: Any): T = decodeRecord(ctx, schema, fieldDecoding, value)

  def withNamespace(namespace: String): DecoderV2[T] = decoder(ctx, schemaFor.fieldMapper, NamespaceUpdate(namespace))

  override def withSchema(schemaFor: SchemaForV2[T]): DecoderV2[T] = {
    verifyNewSchema(schemaFor)
    decoder(ctx, schemaFor.fieldMapper, FullSchemaUpdate(schemaFor))
  }
}

class RecordCodec[T](ctx: CaseClass[Codec, T],
                     val schemaFor: SchemaForV2[T],
                     fieldEncoding: Seq[RecordFields.FieldEncoder[T]],
                     fieldDecoding: Seq[RecordFields.FieldCodec[T]])
    extends Codec[T]
    with NamespaceAware[Codec[T]] {

  def encode(value: T): AnyRef = encodeRecord(ctx, schema, fieldEncoding, value)

  def decode(value: Any): T = decodeRecord(ctx, schema, fieldDecoding, value)

  def withNamespace(namespace: String): Codec[T] = codec(ctx, schemaFor.fieldMapper, NamespaceUpdate(namespace))

  override def withSchema(schemaFor: SchemaForV2[T]): Codec[T] = {
    verifyNewSchema(schemaFor)
    codec(ctx, schemaFor.fieldMapper, FullSchemaUpdate(schemaFor))
  }
}

object Records {

  @inline
  private[avro4s] def encodeRecord[Typeclass[_], T](ctx: CaseClass[Typeclass, T],
                                                    schema: Schema,
                                                    fieldEncoding: Seq[FieldEncoder[T]],
                                                    value: T): AnyRef = {
    // hot code path. Sacrificing functional programming to the gods of performance.
    val length = fieldEncoding.length
    val values = new Array[AnyRef](length)
    var i = 0
    while (i < length) {
      values(i) = fieldEncoding(i).encodeFieldValue(value)
      i += 1
    }
    ImmutableRecord(schema, values)
  }

  @inline
  private[avro4s] def decodeRecord[Typeclass[_], T](ctx: CaseClass[Typeclass, T],
                                                    schema: Schema,
                                                    fieldDecoding: Seq[FieldDecoder],
                                                    value: Any): T =
    value match {
      case record: IndexedRecord =>
        // hot code path. Sacrificing functional programming to the gods of performance.
        val length = fieldDecoding.length
        val values = new Array[Any](length)
        var i = 0
        // testing schema based on reference equality, as value equality on schemas is probably more expensive to check
        // than using safe decoding.
        if (record.getSchema eq schema) {
          // known schema, use fast pre-computed position-based decoding
          while (i < length) {
            values(i) = fieldDecoding(i).fastDecodeFieldValue(record)
            i += 1
          }
        } else {
          // use safe name-based decoding
          while (i < length) {
            values(i) = fieldDecoding(i).safeDecodeFieldValue(record)
            i += 1
          }
        }
        ctx.rawConstruct(values)
      case _ =>
        sys.error(
          s"This decoder can only handle IndexedRecords or its subtypes such as GenericRecord [was ${value.getClass}]")
    }

  private trait Builder[Typeclass[_], T, FieldProcessor[_]] {
    def fieldConstructor: (Param[Typeclass, T], Option[Field]) => FieldProcessor[T]

    def paramSchema: Param[Typeclass, T] => Schema

    def param: FieldProcessor[T] => Param[Typeclass, T]

    def constructor: (CaseClass[Typeclass, T], Seq[FieldProcessor[T]], SchemaForV2[T]) => Typeclass[T]
  }

  private class CodecBuilder[T] extends Builder[Codec, T, RecordFieldCodec] {

    val fieldConstructor = new RecordFieldCodec(_, _)

    val paramSchema = _.typeclass.schema

    val param = _.param

    val constructor = { (ctx, codecs, schemaFor) =>
      val encoding = buildEncoders(codecs, schemaFor)
      val decoding = buildDecoders(codecs, ctx, param)
      new RecordCodec[T](ctx, schemaFor, encoding, decoding)
    }
  }

  private class EncoderBuilder[T] extends Builder[EncoderV2, T, RecordFieldEncoder] {
    val fieldConstructor = new RecordFieldEncoder(_, _)

    val paramSchema = _.typeclass.schema

    val param = _.param

    val constructor = { (ctx, encoders, schemaFor) =>
      val encoding = buildEncoders(encoders, schemaFor)
      new RecordEncoder[T](ctx, schemaFor, encoding)
    }
  }

  private class DecoderBuilder[T] extends Builder[DecoderV2, T, RecordFieldDecoder] {
    val fieldConstructor = new RecordFieldDecoder(_, _)

    val paramSchema = _.typeclass.schema

    val param = _.param

    val constructor = { (ctx, decoders, schemaFor) =>
      val decoding = buildDecoders(decoders, ctx, param)
      new RecordDecoder[T](ctx, schemaFor, decoding)
    }
  }

  private def create[Typeclass[_], T, E[_]](ctx: CaseClass[Typeclass, T],
                                            update: SchemaUpdate,
                                            fieldMapper: FieldMapper,
                                            builder: Builder[Typeclass, T, E]): Typeclass[T] = {
    val schemaFor = buildSchema(ctx, fieldMapper, update, builder)
    val fieldProcessors = buildFields(ctx, schemaFor, builder)
    builder.constructor(ctx, fieldProcessors, schemaFor)
  }

  private[avro4s] def verifyNewSchema[T](schemaFor: SchemaForV2[T]) = {
    val schemaType = schemaFor.schema.getType
    require(schemaType == Schema.Type.RECORD, s"Schema type for record codecs must be RECORD, received $schemaType")
  }

  def encoder[T](ctx: CaseClass[EncoderV2, T],
                 fieldMapper: FieldMapper,
                 update: SchemaUpdate = NoUpdate): EncoderV2[T] =
    create(ctx, update, fieldMapper, new EncoderBuilder[T])

  def decoder[T](ctx: CaseClass[DecoderV2, T],
                 fieldMapper: FieldMapper,
                 update: SchemaUpdate = NoUpdate): DecoderV2[T] =
    create(ctx, update, fieldMapper, new DecoderBuilder[T])

  def codec[T](ctx: CaseClass[Codec, T], fieldMapper: FieldMapper, update: SchemaUpdate = NoUpdate): Codec[T] =
    create(ctx, update, fieldMapper, new CodecBuilder[T])

  private def buildEncoders[T](encoders: Seq[FieldEncoder[T]], schemaFor: SchemaForV2[T]): Seq[FieldEncoder[T]] =
    schemaFor.schema.getFields.asScala.map(f => encoders.find(e => e.field.contains(f)).get).toVector

  private def buildDecoders[Typeclass[_], T, Decoder](decoders: Seq[Decoder],
                                                      ctx: CaseClass[Typeclass, T],
                                                      param: Decoder => Param[Typeclass, T]): Seq[Decoder] =
    ctx.parameters.map(p => decoders.find(decoder => param(decoder) == p).get).toVector

  def buildFields[Typeclass[_], T, F[_]](ctx: CaseClass[Typeclass, T],
                                         schemaFor: SchemaForV2[T],
                                         builder: Builder[Typeclass, T, F]) = {
    val schema = schemaFor.schema
    val fieldMapper = schemaFor.fieldMapper
    ctx.parameters.map { param =>
      val annotations = new AnnotationExtractors(param.annotations)
      val name = annotations.name.getOrElse(fieldMapper.to(param.label))
      val field = if (annotations.transient) None else Option(schema.getField(name))
      builder.fieldConstructor(param, field)
    }
  }

  def buildSchema[Typeclass[_], T, F[_]](ctx: CaseClass[Typeclass, T],
                                         fieldMapper: FieldMapper,
                                         update: SchemaUpdate,
                                         builder: Builder[Typeclass, T, F]): SchemaForV2[T] = {
    update match {
      case FullSchemaUpdate(s)        => s.forType[T]
      case NamespaceUpdate(namespace) => buildSchema(ctx, fieldMapper, Some(namespace), builder.paramSchema)
      case _                          => buildSchema(ctx, fieldMapper, None, builder.paramSchema)
    }
  }

  def buildSchema[Typeclass[_], T, F[_]](ctx: CaseClass[Typeclass, T],
                                         fieldMapper: FieldMapper,
                                         namespaceUpdate: Option[String],
                                         paramSchema: Param[Typeclass, T] => Schema): SchemaForV2[T] = {
    val annotations = new AnnotationExtractors(ctx.annotations)

    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
    val namespace = namespaceUpdate.getOrElse(nameExtractor.namespace)
    val name = nameExtractor.name

    val fields: Seq[Field] = ctx.parameters.flatMap { param =>
      val annotations = new AnnotationExtractors(param.annotations)

      if (annotations.transient) None
      else {
        val doc = valueTypeDoc(ctx, param)
        Some(buildSchemaField(param, paramSchema(param), annotations, namespace, fieldMapper, doc))
      }
    }

    val record =
      Schema.createRecord(name.replaceAll("[^a-zA-Z0-9_]", ""),
                          annotations.doc.orNull,
                          namespace.replaceAll("[^a-zA-Z0-9_.]", ""),
                          false)
    annotations.aliases.foreach(record.addAlias)
    annotations.props.foreach { case (k, v) => record.addProp(k: String, v: AnyRef) }
    record.setFields(fields.asJava)
    SchemaForV2[T](record, fieldMapper)
  }

  private def buildSchemaField[Typeclass[_], T](param: Param[Typeclass, T],
                                                baseSchema: Schema,
                                                extractor: AnnotationExtractors,
                                                containingNamespace: String,
                                                fieldMapper: FieldMapper,
                                                valueTypeDoc: Option[String]): Schema.Field = {

    val extractor = new AnnotationExtractors(param.annotations)
    val doc = extractor.doc.orElse(valueTypeDoc).orNull
    val default: Option[AnyRef] = if (extractor.nodefault) None else param.default.asInstanceOf[Option[AnyRef]]
    val aliases = extractor.aliases
    val props = extractor.props

    // the name could have been overriden with @AvroName, and then must be encoded with the field mapper
    val name = extractor.name.getOrElse(fieldMapper.to(param.label))

    // the default value may be none, in which case it was not defined, or Some(null), in which case it was defined
    // and set to null, or something else, in which case it's a non null value
    val encodedDefault: AnyRef = default match {
      case None        => null
      case Some(None)  => JsonProperties.NULL_VALUE
      case Some(null)  => JsonProperties.NULL_VALUE
      case Some(other) => DefaultResolver(other, baseSchema)
    }

    // if we have annotated with @AvroFixed then we override the type and change it to a Fixed schema
    // if someone puts @AvroFixed on a complex type, it makes no sense, but that's their cross to bear
    val schema = extractor.fixed.fold(baseSchema) { size =>
      SchemaBuilder.fixed(name).doc(doc).namespace(extractor.namespace.getOrElse(containingNamespace)).size(size)
    }

    // if our default value is null, then we should change the type to be nullable even if we didn't use option
    val schemaWithPossibleNull = if (default.contains(null) && schema.getType != Schema.Type.UNION) {
      SchemaBuilder.unionOf().`type`(schema).and().`type`(Schema.create(Schema.Type.NULL)).endUnion()
    } else schema

    // for a union the type that has a default must be first (including null as an explicit default)
    // if there is no default then we'll move null to head (if present)
    // otherwise left as is
    val schemaWithOrderedUnion = (schemaWithPossibleNull.getType, encodedDefault) match {
      case (Schema.Type.UNION, null)                      => SchemaHelper.moveNullToHead(schemaWithPossibleNull)
      case (Schema.Type.UNION, JsonProperties.NULL_VALUE) => SchemaHelper.moveNullToHead(schemaWithPossibleNull)
      case (Schema.Type.UNION, defaultValue)              => SchemaHelper.moveDefaultToHead(schemaWithPossibleNull, defaultValue)
      case _                                              => schemaWithPossibleNull
    }

    // the field can override the containingNamespace if the Namespace annotation is present on the field
    // we may have annotated our field with @AvroNamespace so this containingNamespace should be applied
    // to any schemas we have generated for this field
    val schemaWithResolvedNamespace = extractor.namespace
      .map(SchemaHelper.overrideNamespace(schemaWithOrderedUnion, _))
      .getOrElse(schemaWithOrderedUnion)

    val field = encodedDefault match {
      case null => new Schema.Field(name, schemaWithResolvedNamespace, doc)
      case CustomUnionDefault(_, m) =>
        new Schema.Field(name, schemaWithResolvedNamespace, doc, m)
      case CustomEnumDefault(m) =>
        new Schema.Field(name, schemaWithResolvedNamespace, doc, m)
      case CustomUnionWithEnumDefault(_, _, m) => new Schema.Field(name, schemaWithResolvedNamespace, doc, m)
      case _                                   => new Schema.Field(name, schemaWithResolvedNamespace, doc, encodedDefault)
    }

    props.foreach { case (k, v) => field.addProp(k, v: AnyRef) }
    aliases.foreach(field.addAlias)
    field
  }

  private def valueTypeDoc[Typeclass[_], T](ctx: CaseClass[Typeclass, T], param: Param[Typeclass, T]): Option[String] =
    try {
      // if the field is a value type then we may have annotated it with @AvroDoc, and that doc should be
      // placed onto the field, not onto the record type, because there won't be a record type for a value type!
      // magnolia won't give us the type of the parameter, so we must find it in the class type
      import scala.reflect.runtime.universe
      val mirror = universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
      val sym = mirror.staticClass(ctx.typeName.full).primaryConstructor.asMethod.paramLists.head(param.index)
      sym.typeSignature.typeSymbol.annotations.collectFirst {
        case a if a.tree.tpe =:= typeOf[AvroDoc] =>
          val annoValue = a.tree.children.tail.head.asInstanceOf[Literal].value.value
          annoValue.toString
      }
    } catch {
      case NonFatal(_) => None
    }
}
