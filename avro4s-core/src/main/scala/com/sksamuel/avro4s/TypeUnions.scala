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

  def codec[T](ctx: SealedTrait[Codec, T], update: SchemaUpdate): Codec[T] =
    create(ctx, update, new CodecBuilder[T])

  def encoder[T](ctx: SealedTrait[Encoder, T], update: SchemaUpdate): Encoder[T] =
    create(ctx, update, new EncoderBuilder[T])

  def decoder[T](ctx: SealedTrait[Decoder, T], update: SchemaUpdate): Decoder[T] =
    create(ctx, update, new DecoderBuilder[T])

  def schema[T](ctx: SealedTrait[SchemaFor, T], update: SchemaUpdate): SchemaFor[T] =
    create(ctx, update, new SchemaForBuilder[T](update.fieldMapper))

  private trait Builder[Typeclass[_], T, Entry[_]] {
    def entryConstructor: (Subtype[Typeclass, T], SchemaUpdate) => Entry[T]

    def entrySchema: Entry[T] => Schema

    def constructor: (SealedTrait[Typeclass, T], Seq[Entry[T]], SchemaFor[T]) => Typeclass[T]
  }

  private class CodecBuilder[T] extends Builder[Codec, T, UnionEntryCodec] {

    val entryConstructor = new UnionEntryCodec[T](_, _)

    val entrySchema = _.schema

    val constructor = { (ctx, subtypeCodecs, schemaFor) =>
      val codecByName = subtypeCodecs.map(c => c.fullName -> c).toMap
      val codecBySubtype = subtypeCodecs.map(c => c.st -> c).toMap
      new TypeUnionCodec(ctx, schemaFor, codecByName, codecBySubtype)
    }
  }

  private class EncoderBuilder[T] extends Builder[Encoder, T, UnionEntryEncoder] {
    val entryConstructor = new UnionEntryEncoder[T](_, _)

    val entrySchema = _.schema

    val constructor = { (ctx, subtypeEncoder, schemaFor) =>
      val encoderBySubtype = subtypeEncoder.map(c => c.st -> c).toMap
      new TypeUnionEncoder(ctx, schemaFor, encoderBySubtype)
    }
  }

  private class DecoderBuilder[T] extends Builder[Decoder, T, UnionEntryDecoder] {
    val entryConstructor = new UnionEntryDecoder[T](_, _)

    val entrySchema = _.schema

    val constructor = { (ctx, subtypeDecoder, schemaFor) =>
      val decoderByName = subtypeDecoder.map(decoder => decoder.fullName -> decoder).toMap
      new TypeUnionDecoder(ctx, schemaFor, decoderByName)
    }
  }

  private class SchemaForBuilder[T](fieldMapper: FieldMapper) extends Builder[SchemaFor, T, SchemaFor] {

    def entryConstructor: (Subtype[SchemaFor, T], SchemaUpdate) => SchemaFor[T] = { (st, u) =>
      u match {
        case NamespaceUpdate(ns, fm) =>
          val oldSchemaFor = st.typeclass
          SchemaFor(SchemaHelper.overrideNamespace(oldSchemaFor.schema, ns), fm)

        case FullSchemaUpdate(schemaFor) => schemaFor.forType[T]
        case UseFieldMapper(fm)          => st.typeclass.copy[T](fieldMapper = fm)
      }
    }

    val entrySchema = _.schema

    def constructor: (SealedTrait[SchemaFor, T], Seq[SchemaFor[T]], SchemaFor[T]) => SchemaFor[T] =
      (_, schemaFors, _) => SchemaFor[T](SchemaHelper.createSafeUnion(schemaFors.map(_.schema): _*), fieldMapper)
  }

  private def create[Typeclass[_], T, E[_]](ctx: SealedTrait[Typeclass, T],
                                            update: SchemaUpdate,
                                            builder: Builder[Typeclass, T, E]): Typeclass[T] = {
    val extendedUpdate = update match {
      case UseFieldMapper(fm) =>
        val ns = new AnnotationExtractors(ctx.annotations).namespace
        ns.fold[SchemaUpdate](UseFieldMapper(fm))(NamespaceUpdate(_, fm))
      case _ => update
    }
    val subtypeEntries = buildSubtypeEntries(ctx, extendedUpdate, builder)
    val schema = buildSchema(ctx, extendedUpdate, subtypeEntries, builder)
    builder.constructor(ctx, subtypeEntries, schema)
  }

  private[avro4s] def buildSubtypeEntries[Typeclass[_], T, E[_]](ctx: SealedTrait[Typeclass, T],
                                                                 overrides: SchemaUpdate,
                                                                 builder: Builder[Typeclass, T, E]): Seq[E[T]] = {

    def subtypeOverride(st: Subtype[Typeclass, T]) = overrides match {
      case FullSchemaUpdate(schemaFor) =>
        val schema = schemaFor.schema
        val fieldMapper = schemaFor.fieldMapper
        val nameExtractor = NameExtractor(st.typeName, st.annotations ++ ctx.annotations)
        val subtraitSchema =
          SchemaFor(SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema), fieldMapper)
        FullSchemaUpdate(subtraitSchema)
      case _ => overrides
    }

    sortedSubtypes(ctx).map { st: Subtype[Typeclass, T] =>
      builder.entryConstructor(st, subtypeOverride(st))
    }
  }

  private[avro4s] def validateNewSchema[T](schemaFor: SchemaFor[T]) = {
    val newSchema = schemaFor.schema
    require(newSchema.getType == Schema.Type.UNION,
            s"Schema type for record codecs must be UNION, received ${newSchema.getType}")
  }

  def sortedSubtypes[Typeclass[_], T](ctx: SealedTrait[Typeclass, T]) = {
    def priority(st: Subtype[Typeclass, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    ctx.subtypes.sortWith((l, r) => priority(l) > priority(r))
  }

  def buildSchema[Typeclass[_], T, E[_]](ctx: SealedTrait[Typeclass, T],
                                         update: SchemaUpdate,
                                         entries: Seq[E[T]],
                                         builder: Builder[Typeclass, T, E]): SchemaFor[T] = {
    update match {
      case FullSchemaUpdate(s) => s.forType
      case _ =>
        val entryMap = ctx.subtypes.zip(entries).toMap
        val subtypes  = sortedSubtypes(ctx)
        SchemaFor(SchemaHelper.createSafeUnion(subtypes.map(entryMap).map(builder.entrySchema): _*))
    }
  }
}
