package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, UseFieldMapper}
import com.sksamuel.avro4s.TypeUnionEntry._
import com.sksamuel.avro4s.TypeUnions._
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

class TypeUnionCodec[T](ctx: SealedTrait[Codec, T],
                        val schemaFor: SchemaFor[T],
                        codecByName: Map[String, EntryDecoder[T]],
                        codecBySubtype: Map[Subtype[Codec, T], EntryEncoder[T]])
    extends Codec[T]
    with NamespaceAware[Codec[T]] {

  def withNamespace(namespace: String): Codec[T] =
    TypeUnions.codec(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaFor[T]): Codec[T] = {
    validateNewSchema(schemaFor)
    TypeUnions.codec(ctx, FullSchemaUpdate(schemaFor))
  }

  def encode(value: T): AnyRef = encodeUnion(ctx, codecBySubtype, value)

  def decode(value: Any): T = decodeUnion(ctx, codecByName, value)
}

class TypeUnionEncoder[T](ctx: SealedTrait[Encoder, T],
                          val schemaFor: SchemaFor[T],
                          encoderBySubtype: Map[Subtype[Encoder, T], EntryEncoder[T]])
    extends Encoder[T]
    with NamespaceAware[Encoder[T]] {

  def withNamespace(namespace: String): Encoder[T] =
    TypeUnions.encoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    validateNewSchema(schemaFor)
    TypeUnions.encoder(ctx, FullSchemaUpdate(schemaFor))
  }

  def encode(value: T): AnyRef = encodeUnion(ctx, encoderBySubtype, value)
}

class TypeUnionDecoder[T](ctx: SealedTrait[Decoder, T],
                          val schemaFor: SchemaFor[T],
                          decoderByName: Map[String, EntryDecoder[T]])
    extends Decoder[T]
    with NamespaceAware[Decoder[T]] {

  def withNamespace(namespace: String): Decoder[T] =
    TypeUnions.decoder(ctx, NamespaceUpdate(namespace, schemaFor.fieldMapper))

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    validateNewSchema(schemaFor)
    TypeUnions.decoder(ctx, FullSchemaUpdate(schemaFor))
  }

  def decode(value: Any): T = decodeUnion(ctx, decoderByName, value)
}

object TypeUnions {

  @inline
  private[avro4s] def encodeUnion[Typeclass[_], T](ctx: SealedTrait[Typeclass, T],
                                                   encoderBySubtype: Map[Subtype[Typeclass, T], EntryEncoder[T]],
                                                   value: T): AnyRef =
    // we need an additional indirection since we may have enhanced the original magnolia-provided encoder via annotations
    ctx.dispatch(value)(subtype => encoderBySubtype(subtype).encodeSubtype(value))

  @inline
  private[avro4s] def decodeUnion[Typeclass[_], T](ctx: SealedTrait[Typeclass, T],
                                                   decoderByName: Map[String, EntryDecoder[T]],
                                                   value: Any) =
    value match {
      case container: GenericContainer =>
        val schemaName = container.getSchema.getFullName
        val codecOpt = decoderByName.get(schemaName)
        if (codecOpt.isDefined) {
          codecOpt.get.decodeSubtype(container)
        } else {
          val schemaNames = decoderByName.keys.toSeq.sorted.mkString("[", ", ", "]")
          sys.error(s"Could not find schema $schemaName in type union schemas $schemaNames")
        }
      case _ => sys.error(s"Unsupported type $value in type union decoder")
    }

  trait EntryDecoder[T] extends Serializable {
    def decodeSubtype(value: Any): T
  }

  trait EntryEncoder[T] extends Serializable {
    def encodeSubtype(value: T): AnyRef
  }

  trait EntryCodec[T] extends EntryDecoder[T] with EntryEncoder[T]

  def codec[T](ctx: SealedTrait[Codec, T], update: SchemaUpdate): Codec[T] = {
    val subtypeCodecs = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionEntryCodec[T](st, u) }
    val schemaFor = buildSchema[T](update, subtypeCodecs.map(_.schema))
    val codecByName = subtypeCodecs.map(c => c.fullName -> c).toMap
    val codecBySubtype = subtypeCodecs.map(c => c.st -> c).toMap
    new TypeUnionCodec(ctx, schemaFor, codecByName, codecBySubtype)
  }

  def encoder[T](ctx: SealedTrait[Encoder, T], update: SchemaUpdate): Encoder[T] = {
    val subtypeEncoders = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionEntryEncoder[T](st, u) }
    val schemaFor = buildSchema[T](update, subtypeEncoders.map(_.schema))
    val encoderBySubtype = subtypeEncoders.map(c => c.st -> c).toMap
    new TypeUnionEncoder(ctx, schemaFor, encoderBySubtype)
  }

  def decoder[T](ctx: SealedTrait[Decoder, T], update: SchemaUpdate): Decoder[T] = {
    val subtypeDecoders = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionEntryDecoder[T](st, u) }
    val schemaFor = buildSchema[T](update, subtypeDecoders.map(_.schema))
    val decoderByName = subtypeDecoders.map(decoder => decoder.fullName -> decoder).toMap
    new TypeUnionDecoder(ctx, schemaFor, decoderByName)
  }

  def schema[T](ctx: SealedTrait[SchemaFor, T], update: SchemaUpdate): SchemaFor[T] = {
    val subtypeSchema: ((Subtype[SchemaFor, T], SchemaUpdate)) => Schema = {
      case (st, u) =>
        u match {
          case NamespaceUpdate(ns, _) =>
            val oldSchemaFor = st.typeclass
            SchemaHelper.overrideNamespace(oldSchemaFor.schema, ns)

          case FullSchemaUpdate(schemaFor) => schemaFor.schema
          case UseFieldMapper(fm)          => st.typeclass.schema
        }
    }

    val subtypeSchemas = enrichedSubtypes(ctx, update).map(subtypeSchema)
    SchemaFor[T](SchemaHelper.createSafeUnion(subtypeSchemas: _*), update.fieldMapper)
  }

  private def enrichedSubtypes[Typeclass[_], T](ctx: SealedTrait[Typeclass, T],
                                                update: SchemaUpdate): Seq[(Subtype[Typeclass, T], SchemaUpdate)] = {
    val enrichedUpdate = update match {
      case UseFieldMapper(fm) =>
        val ns = new AnnotationExtractors(ctx.annotations).namespace
        ns.fold[SchemaUpdate](UseFieldMapper(fm))(NamespaceUpdate(_, fm))
      case _ => update
    }

    def subtypeSchemaUpdate(st: Subtype[Typeclass, T]) = enrichedUpdate match {
      case FullSchemaUpdate(schemaFor) =>
        val schema = schemaFor.schema
        val fieldMapper = schemaFor.fieldMapper
        val nameExtractor = NameExtractor(st.typeName, st.annotations ++ ctx.annotations)
        val subtraitSchema =
          SchemaFor(SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema), fieldMapper)
        FullSchemaUpdate(subtraitSchema)
      case _ => enrichedUpdate
    }

    def priority(st: Subtype[Typeclass, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    val sortedSubtypes = ctx.subtypes.sortWith((l, r) => priority(l) > priority(r))

    sortedSubtypes.map(st => (st, subtypeSchemaUpdate(st)))
  }

  private[avro4s] def validateNewSchema[T](schemaFor: SchemaFor[T]) = {
    val newSchema = schemaFor.schema
    require(newSchema.getType == Schema.Type.UNION,
            s"Schema type for record codecs must be UNION, received ${newSchema.getType}")
  }

  def buildSchema[T](update: SchemaUpdate, schemas: Seq[Schema]): SchemaFor[T] = update match {
    case FullSchemaUpdate(s) => s.forType
    case _                   => SchemaFor(SchemaHelper.createSafeUnion(schemas: _*))
  }
}
