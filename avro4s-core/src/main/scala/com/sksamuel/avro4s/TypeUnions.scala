package com.sksamuel.avro4s

import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
import com.sksamuel.avro4s.TypeUnionEntry._
import com.sksamuel.avro4s.TypeUnions._
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

class TypeUnionEncoder[T](ctx: SealedTrait[Encoder, T],
                          val schemaFor: SchemaFor[T],
                          encoderBySubtype: Map[Subtype[Encoder, T], UnionEncoder[T]#SubtypeEncoder])
    extends Encoder[T] {

  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    validateNewSchema(schemaFor)
    TypeUnions.encoder(ctx, new DefinitionEnvironment[Encoder](), FullSchemaUpdate(schemaFor))
  }

  def encode(value: T): AnyRef =
    // we need an additional indirection since we may have enhanced the original magnolia-provided encoder via annotations
    ctx.dispatch(value)(subtype => encoderBySubtype(subtype).encodeSubtype(value))
}

class TypeUnionDecoder[T](ctx: SealedTrait[Decoder, T],
                          val schemaFor: SchemaFor[T],
                          decoderByName: Map[String, UnionDecoder[T]#SubtypeDecoder])
    extends Decoder[T] {

  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
    validateNewSchema(schemaFor)
    TypeUnions.decoder(ctx, new DefinitionEnvironment[Decoder](), FullSchemaUpdate(schemaFor))
  }

  def decode(value: Any): T = value match {
    case container: GenericContainer =>
      val schemaName = container.getSchema.getFullName
      val codecOpt = decoderByName.get(schemaName)
      if (codecOpt.isDefined) {
        codecOpt.get.decodeSubtype(container)
      } else {
        val schemaNames = decoderByName.keys.toSeq.sorted.mkString("[", ", ", "]")
        throw new Avro4sDecodingException(s"Could not find schema $schemaName in type union schemas $schemaNames", value, this)
      }
    case _ => throw new Avro4sDecodingException(s"Unsupported type $value in type union decoder", value, this)
  }
}

object TypeUnions {

  def encoder[T](ctx: SealedTrait[Encoder, T],
                 env: DefinitionEnvironment[Encoder],
                 update: SchemaUpdate): Encoder[T] = {
    // cannot extend the recursive environment with an initial type union encoder with empty union schema, as Avro Schema
    // doesn't support this. So we use the original recursive environment to build subtypes, meaning that in case of a
    // recursive schema, two identical type union encoders may be created instead of one.
    val subtypeEncoders = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionEncoder[T](st)(env, u) }
    val schemaFor = buildSchema[T](update, subtypeEncoders.map(_.schema))
    val encoderBySubtype = subtypeEncoders.map(e => e.subtype -> e).toMap
    new TypeUnionEncoder[T](ctx, schemaFor, encoderBySubtype)
  }

  def decoder[T](ctx: SealedTrait[Decoder, T],
                 env: DefinitionEnvironment[Decoder],
                 update: SchemaUpdate): Decoder[T] = {
    // cannot extend the recursive environment with an initial type union decoder with empty union schema, as Avro Schema
    // doesn't support this. So we use the original recursive environment to build subtypes, meaning that in case of a
    // recursive schema, two identical type union decoders may be created instead of one.
    val subtypeDecoders = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionDecoder[T](st)(env, u) }
    val schemaFor = buildSchema[T](update, subtypeDecoders.map(_.schema))
    val decoderByName = subtypeDecoders.map(decoder => decoder.fullName -> decoder).toMap
    new TypeUnionDecoder[T](ctx, schemaFor, decoderByName)
  }

  def schema[T](ctx: SealedTrait[SchemaFor, T],
                env: DefinitionEnvironment[SchemaFor],
                update: SchemaUpdate): SchemaFor[T] = {
    val subtypeSchemas = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionSchemaFor[T](st)(env, u) }
    buildSchema[T](update, subtypeSchemas.map(_.schema))
  }

  private def enrichedSubtypes[Typeclass[_], T](ctx: SealedTrait[Typeclass, T],
                                                update: SchemaUpdate): Seq[(Subtype[Typeclass, T], SchemaUpdate)] = {
    val enrichedUpdate = update match {
      case NoUpdate =>
        // in case of namespace annotations, pass the namespace update down to all subtypes
        val ns = new AnnotationExtractors(ctx.annotations).namespace
        ns.fold[SchemaUpdate](NoUpdate)(NamespaceUpdate)
      case _ => update
    }

    def subtypeSchemaUpdate(st: Subtype[Typeclass, T]) = enrichedUpdate match {
      case FullSchemaUpdate(schemaFor) =>
        val schema = schemaFor.schema
        val fieldMapper = schemaFor.fieldMapper
        val nameExtractor = NameExtractor(st.typeName, st.annotations ++ ctx.annotations)
        val subtypeSchema = SchemaFor(SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema), fieldMapper)
        FullSchemaUpdate(subtypeSchema)
      case _ => enrichedUpdate
    }

    def priority(st: Subtype[Typeclass, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    val sortedSubtypes = ctx.subtypes.sortWith((l, r) => priority(l) > priority(r))

    sortedSubtypes.map(st => (st, subtypeSchemaUpdate(st)))
  }

  private[avro4s] def validateNewSchema[T](schemaFor: SchemaFor[T]) = {
    val newSchema = schemaFor.schema
    if(newSchema.getType != Schema.Type.UNION)
      throw new Avro4sConfigurationException(s"Schema type for record codecs must be UNION, received $newSchema")
  }

  def buildSchema[T](update: SchemaUpdate, schemas: Seq[Schema]): SchemaFor[T] = update match {
    case FullSchemaUpdate(s) => s.forType
    case _                   => SchemaFor(SchemaHelper.createSafeUnion(schemas: _*), DefaultFieldMapper)
  }
}
