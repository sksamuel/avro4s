package com.sksamuel.avro4s

import com.sksamuel.avro4s.Codec.Typeclass
import com.sksamuel.avro4s.RecordCodec.buildFieldCodecs
import magnolia.{CaseClass, Param}
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{JsonProperties, Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

class RecordCodec[T](ctx: CaseClass[Typeclass, T], val schema: Schema, fieldMapper: FieldMapper) extends Codec[T] {

  private val (fieldEncoding: Array[RecordCodec.FieldCodec[T]], fieldDecoding: Array[RecordCodec.FieldCodec[T]]) = {
    val codecs = buildFieldCodecs(ctx, schema, fieldMapper)

    val encoding = schema.getFields.asScala.map(f => codecs.find(_.field.exists(_ == f)).get).toArray
    val decoding = ctx.parameters.map(p => codecs.find(_.param == p).get).toArray
    (encoding, decoding)
  }

  override def withSchema(schema: Schema): Codec[T] = new RecordCodec[T](ctx, schema, fieldMapper)

  def encode(value: T): AnyRef = {
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

  def decode(value: Any): T = value match {
    case record: IndexedRecord =>
      // hot code path. Sacrificing functional programming to the gods of performance.
      val length = fieldDecoding.length
      val values = new Array[Any](length)
      var i = 0
      while (i < length) {
        values(i) = fieldDecoding(i).decodeFieldValue(record)
        i += 1
      }
      ctx.rawConstruct(values)
    case _ =>
      sys.error(
        s"This decoder can only handle IndexedRecords or its subtypes such as GenericRecord [was ${value.getClass}]")
  }

}

object RecordCodec {

  def apply[T](ctx: CaseClass[Typeclass, T], fieldMapper: FieldMapper): RecordCodec[T] = {
    val schema = buildSchema(ctx, fieldMapper)
    new RecordCodec[T](ctx, schema, fieldMapper)
  }

  class FieldCodec[T](val param: Param[Typeclass, T], val field: Option[Field]) {

    private val codec: Typeclass[param.PType] = {
      val codec = param.typeclass
      field.map(f => if (f.schema != codec.schema) codec.withSchema(f.schema) else codec).getOrElse(codec)
    }

    private val fieldPosition = field.map(_.pos).getOrElse(-1)

    def encodeFieldValue(t: T): AnyRef = codec.encode(param.dereference(t))

    def decodeFieldValue(record: IndexedRecord): param.PType =
      if (fieldPosition == -1) {
        param.default match {
          case Some(default) => default
          // there is no default, so the field must be an option
          case None => codec.decode(null)
        }
      } else {
        val value = record.get(fieldPosition)
        try {
          codec.decode(value)
        } catch {
          case NonFatal(ex) => param.default.getOrElse(throw ex)
        }
      }
  }

  def buildFieldCodecs[T](ctx: CaseClass[Typeclass, T], schema: Schema, fieldMapper: FieldMapper): Seq[FieldCodec[T]] =
    ctx.parameters.map { param =>
      val annotations = new AnnotationExtractors(param.annotations)
      val name = annotations.name.getOrElse(fieldMapper.to(param.label))
      val field = if (annotations.transient) None else Option(schema.getField(name))
      new FieldCodec(param, field)
    }

  private def buildSchema[T](ctx: CaseClass[Typeclass, T], fieldMapper: FieldMapper): Schema = {
    val annotations = new AnnotationExtractors(ctx.annotations)
    val doc = annotations.doc.orNull
    val aliases = annotations.aliases
    val props = annotations.props

    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
    val namespace = nameExtractor.namespace
    val name = nameExtractor.name

    val fields: Seq[Field] = ctx.parameters.flatMap { param =>
      val annotations = new AnnotationExtractors(param.annotations)

      if (annotations.transient) None
      else {
        val doc = valueTypeDoc(ctx, param)
        Some(buildSchemaField(param, annotations, namespace, fieldMapper, doc))
      }
    }

    val record =
      Schema.createRecord(name.replaceAll("[^a-zA-Z0-9_]", ""), doc, namespace.replaceAll("[^a-zA-Z0-9_.]", ""), false)
    aliases.foreach(record.addAlias)
    props.foreach { case (k, v) => record.addProp(k: String, v: AnyRef) }
    record.setFields(fields.asJava)
    record
  }

  private def buildSchemaField[T](param: Param[Typeclass, T],
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

    val baseSchema = param.typeclass.schema

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

  private def valueTypeDoc[T](ctx: CaseClass[Typeclass, T], param: Param[Typeclass, T]): Option[String] =
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
