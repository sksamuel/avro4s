package com.sksamuel.avro4s

import com.sksamuel.avro4s.RecordFields._
import com.sksamuel.avro4s.Records._
import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, UseFieldMapper}
import magnolia.{CaseClass, Param}
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{JsonProperties, Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

class RecordEncoder[T](ctx: CaseClass[Encoder, T],
                       val schemaFor: SchemaFor[T],
                       fieldEncoding: IndexedSeq[RecordFieldEncoder[T]])
    extends Encoder[T]
    with NamespaceAware[Encoder[T]] {

  def encode(value: T): AnyRef = {
    // hot code path. Sacrificing functional programming to the gods of performance.
    val length = fieldEncoding.length
    val values = new Array[AnyRef](length)
    var i = 0
    while (i < length) {
      values(i) = fieldEncoding(i).encodeFieldValue(value)
      i += 1
    }
    ImmutableRecord(schema, values) // note: array gets implicitly wrapped in an immutable container.
  }

  def withNamespace(namespace: String): Encoder[T] = encoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    verifyNewSchema(schemaFor)
    encoder(ctx, FullSchemaUpdate(schemaFor))
  }
}

class RecordDecoder[T](ctx: CaseClass[Decoder, T], val schemaFor: SchemaFor[T], fieldDecoding: IndexedSeq[RecordFieldDecoder[T]])
    extends Decoder[T]
    with NamespaceAware[Decoder[T]] {

  def decode(value: Any): T = value match {
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

  def withNamespace(namespace: String): Decoder[T] =
    decoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    verifyNewSchema(schemaFor)
    decoder(ctx, FullSchemaUpdate(schemaFor))
  }
}

object Records {

  private[avro4s] def verifyNewSchema[T](schemaFor: SchemaFor[T]) = {
    val schemaType = schemaFor.schema.getType
    require(schemaType == Schema.Type.RECORD, s"Schema type for record codecs must be RECORD, received $schemaType")
  }

  def encoder[T](ctx: CaseClass[Encoder, T], update: SchemaUpdate): Encoder[T] = {
    val paramSchema = ctx.parameters.map(p => p -> p.typeclass.schema).toMap
    val schemaFor = buildSchema(ctx, update, paramSchema)
    val encoders = paramFields(ctx, schemaFor).map { case (param, field) => new RecordFieldEncoder(param, field) }
    new RecordEncoder[T](ctx, schemaFor, reorderEncoders(encoders, schemaFor))
  }

  def decoder[T](ctx: CaseClass[Decoder, T], update: SchemaUpdate): Decoder[T] = {
    val paramSchema = ctx.parameters.map(p => p -> p.typeclass.schema).toMap
    val schemaFor = buildSchema(ctx, update, paramSchema)
    val decoders = paramFields(ctx, schemaFor).map { case (param, field) => new RecordFieldDecoder(param, field) }.toVector
    new RecordDecoder[T](ctx, schemaFor, decoders)
  }

  private def reorderEncoders[T](encoders: Seq[RecordFieldEncoder[T]], schemaFor: SchemaFor[T]): IndexedSeq[RecordFieldEncoder[T]] =
    schemaFor.schema.getFields.asScala.map(f => encoders.find(e => e.field.contains(f)).get).toVector

  private[avro4s] def paramFields[Typeclass[_], T](ctx: CaseClass[Typeclass, T], schemaFor: SchemaFor[T]) = {
    val schema = schemaFor.schema
    val fieldMapper = schemaFor.fieldMapper
    ctx.parameters.map { param =>
      val annotations = new AnnotationExtractors(param.annotations)
      val name = annotations.name.getOrElse(fieldMapper.to(param.label))
      val field = if (annotations.transient) None else Option(schema.getField(name))
      param -> field
    }
  }

  private def buildSchema[Typeclass[_], T, F[_]](ctx: CaseClass[Typeclass, T],
                                         update: SchemaUpdate,
                                         paramSchema: Param[Typeclass, T] => Schema): SchemaFor[T] = update match {
    case FullSchemaUpdate(s)                     => s.forType[T]
    case NamespaceUpdate(namespace, fieldMapper) => schema(ctx, fieldMapper, Some(namespace), paramSchema)
    case UseFieldMapper(fieldMapper)             => schema(ctx, fieldMapper, None, paramSchema)
  }

  def schema[Typeclass[_], T, F[_]](ctx: CaseClass[Typeclass, T],
                                    fieldMapper: FieldMapper,
                                    namespaceUpdate: Option[String],
                                    paramSchema: Param[Typeclass, T] => Schema): SchemaFor[T] = {
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
    SchemaFor[T](record, fieldMapper)
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
