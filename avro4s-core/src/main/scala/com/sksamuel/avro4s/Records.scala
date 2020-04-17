package com.sksamuel.avro4s

import com.sksamuel.avro4s.RecordFields._
import com.sksamuel.avro4s.Records._
import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
import magnolia.{CaseClass, Param}
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{JsonProperties, Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

class RecordEncoder[T: WeakTypeTag](ctx: CaseClass[Encoder, T], val schemaFor: SchemaFor[T]) extends Encoder[T] {

  private[avro4s] var fieldEncoding: IndexedSeq[FieldEncoder[T]#ValueEncoder] = IndexedSeq.empty

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

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    verifyNewSchema(schemaFor)
    encoder(ctx, new DefinitionEnvironment[Encoder](), FullSchemaUpdate(schemaFor), schemaFor.fieldMapper)
  }
}

class RecordDecoder[T: WeakTypeTag](ctx: CaseClass[Decoder, T], val schemaFor: SchemaFor[T]) extends Decoder[T] {

  private[avro4s] var fieldDecoding: IndexedSeq[FieldDecoder[T]#ValueDecoder] = IndexedSeq.empty

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

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    verifyNewSchema(schemaFor)
    decoder(ctx, new DefinitionEnvironment[Decoder](), FullSchemaUpdate(schemaFor), schemaFor.fieldMapper)
  }
}

object Records {

  private[avro4s] def verifyNewSchema[T](schemaFor: SchemaFor[T]) = {
    val schemaType = schemaFor.schema.getType
    require(schemaType == Schema.Type.RECORD, s"Schema type for record codecs must be RECORD, received $schemaType")
  }

  def encoder[T: WeakTypeTag](ctx: CaseClass[Encoder, T],
                              env: DefinitionEnvironment[Encoder],
                              update: SchemaUpdate,
                              fieldMapper: FieldMapper): Encoder[T] = {
    val record = buildRecordSchema(ctx, update)
    val schemaFor = SchemaFor[T](record, fieldMapper)
    val encoder = new RecordEncoder[T](ctx, schemaFor)
    val nextEnv = env.updated(encoder)
    val fieldsAndEncoders = ctx.parameters.flatMap { param =>
      val annotations = new AnnotationExtractors(param.annotations)
      if (annotations.transient) None else Some(new FieldEncoder(param)(nextEnv, update, record, ctx, fieldMapper))
    }
    val encoders = fieldsAndEncoders.map(_._2)
    update match {
      case FullSchemaUpdate(_) =>
        record.getFields.asScala.map(f => encoders.find(e => e.fieldName == f.name).get).toVector
        encoder.fieldEncoding = encoders.toVector

      case _ =>
        val fields = fieldsAndEncoders.map(_._1)
        record.setFields(fields.asJava)
        encoder.fieldEncoding = encoders.toVector
    }
    encoder
  }

  def decoder[T: WeakTypeTag](ctx: CaseClass[Decoder, T],
                              env: DefinitionEnvironment[Decoder],
                              update: SchemaUpdate,
                              fieldMapper: FieldMapper): Decoder[T] = {
    val record = buildRecordSchema(ctx, update)
    val schemaFor = SchemaFor[T](record, fieldMapper)
    val decoder = new RecordDecoder[T](ctx, schemaFor)
    val nextEnv = env.updated(decoder)
    val fieldsAndDecoders = ctx.parameters.zipWithIndex.map {
      case (param, idx) =>
        new FieldDecoder[T](param)(idx, nextEnv, update, record, ctx, fieldMapper)
    }
    val decoders = fieldsAndDecoders.map(_._2)
    update match {
      case FullSchemaUpdate(_) =>
        record.getFields.asScala.map(f => decoders.find(e => e.fieldName.contains(f.name)).get).toVector
        decoder.fieldDecoding = decoders.toVector

      case _ =>
        val fields = fieldsAndDecoders.flatMap(_._1)
        record.setFields(fields.asJava)
        decoder.fieldDecoding = decoders.toVector
    }
    decoder
  }

  def schema[T: WeakTypeTag](ctx: CaseClass[SchemaFor, T],
                             env: DefinitionEnvironment[SchemaFor],
                             update: SchemaUpdate,
                             fieldMapper: FieldMapper): SchemaFor[T] = {
    val record = buildRecordSchema(ctx, update)
    // early incomplete construction here for potentially recursive schema definitions
    val schemaFor = SchemaFor[T](record, fieldMapper)
    val nextEnv = env.updated(schemaFor)
    val fields = ctx.parameters.flatMap { param =>
      val annotations = new AnnotationExtractors(param.annotations)
      if (annotations.transient) None
      else {
        val doc = valueTypeDoc(ctx, param)
        val schema = param.typeclass.apply(nextEnv, NoUpdate).schema
        Some(buildSchemaField(param, schema, annotations, record.getNamespace, fieldMapper, doc))
      }
    }.asJava
    record.setFields(fields)
    schemaFor
  }

  def buildRecordSchema[Typeclass[_]](ctx: CaseClass[Typeclass, _], update: SchemaUpdate): Schema = {
    def newSchema(namespaceUpdate: Option[String]) = {
      val annotations = new AnnotationExtractors(ctx.annotations)

      val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
      val namespace = namespaceUpdate.getOrElse(nameExtractor.namespace)
      val name = nameExtractor.name

      val record =
        Schema.createRecord(name.replaceAll("[^a-zA-Z0-9_]", ""),
                            annotations.doc.orNull,
                            namespace.replaceAll("[^a-zA-Z0-9_.]", ""),
                            false)
      annotations.aliases.foreach(record.addAlias)
      annotations.props.foreach { case (k, v) => record.addProp(k: String, v: AnyRef) }
      record
    }

    update match {
      case FullSchemaUpdate(sf) =>
        require(sf.schema.getType == Schema.Type.RECORD,
                s"Schema for case classes must be of type Record, got ${sf.schema.getType}")
        sf.schema
      case NamespaceUpdate(namespace) => newSchema(Some(namespace))
      case _                          => newSchema(None)
    }
  }

  private[avro4s] def buildSchemaField[Typeclass[_], T](param: Param[Typeclass, T],
                                                        baseSchema: Schema,
                                                        extractor: AnnotationExtractors,
                                                        containingNamespace: String,
                                                        fieldMapper: FieldMapper,
                                                        valueTypeDoc: Option[String]): Schema.Field = {
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

  private[avro4s] def valueTypeDoc[Typeclass[_], _](ctx: CaseClass[Typeclass, _],
                                                    param: Param[Typeclass, _]): Option[String] =
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
