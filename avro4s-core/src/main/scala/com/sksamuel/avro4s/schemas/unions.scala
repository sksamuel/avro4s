package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.avroutils.SchemaHelper
import com.sksamuel.avro4s.typeutils.{Annotations, SubtypeOrdering}
import magnolia1.SealedTrait
import org.apache.avro.SchemaBuilder

object TypeUnions {

  //  def decoder[T](ctx: SealedTrait[Decoder, T],
  //                 env: DefinitionEnvironment[Decoder],
  //                 update: SchemaUpdate): Decoder[T] = {
  //    // cannot extend the recursive environment with an initial type union decoder with empty union schema, as Avro Schema
  //    // doesn't support this. So we use the original recursive environment to build subtypes, meaning that in case of a
  //    // recursive schema, two identical type union decoders may be created instead of one.
  //    val subtypeDecoders = enrichedSubtypes(ctx, update).map { case (st, u) => new UnionDecoder[T](st)(env, u) }
  //    val schemaFor = buildSchema[T](update, subtypeDecoders.map(_.schema))
  //    val decoderByName = subtypeDecoders.map(decoder => decoder.fullName -> decoder).toMap
  //    new TypeUnionDecoder[T](ctx, schemaFor, decoderByName)
  //  }

  def schema[T](ctx: SealedTrait[SchemaFor, T]): SchemaFor[T] = {
    val schemas = Seq(SchemaBuilder.builder().booleanType(), SchemaBuilder.builder().intType())
    val sortedSubtypes = ctx.subtypes.sorted(SubtypeOrdering).map(_.typeclass.schema)
    SchemaFor(SchemaHelper.createSafeUnion(sortedSubtypes: _*))
  }

  //
  //  private def enrichedSubtypes[Typeclass[_], T](ctx: SealedTrait[Typeclass, T],
  //                                                update: SchemaUpdate): Seq[(Subtype[Typeclass, T], SchemaUpdate)] = {
  //    val enrichedUpdate = update match {
  //      case NoUpdate =>
  //        // in case of namespace annotations, pass the namespace update down to all subtypes
  //        val ns = new AnnotationExtractors(ctx.annotations).namespace
  //        ns.fold[SchemaUpdate](NoUpdate)(NamespaceUpdate)
  //      case _ => update
  //    }
  //
  //    def subtypeSchemaUpdate(st: Subtype[Typeclass, T]) = enrichedUpdate match {
  //      case FullSchemaUpdate(schemaFor) =>
  //        val schema = schemaFor.schema
  //        val fieldMapper = schemaFor.fieldMapper
  //        val nameExtractor = NameExtractor(st.typeName, st.annotations ++ ctx.annotations)
  //        val subtypeSchema = SchemaFor(SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema), fieldMapper)
  //        FullSchemaUpdate(subtypeSchema)
  //      case _ => enrichedUpdate
  //    }
  //
  //    def priority(st: Subtype[Typeclass, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
  //    val sortedSubtypes = ctx.subtypes.sortWith((l, r) => priority(l) > priority(r))
  //
  //    sortedSubtypes.map(st => (st, subtypeSchemaUpdate(st)))
  //  }
  //
  //  private[avro4s] def validateNewSchema[T](schemaFor: SchemaFor[T]) = {
  //    val newSchema = schemaFor.schema
  //    if (newSchema.getType != Schema.Type.UNION)
  //      throw new Avro4sConfigurationException(s"Schema type for record codecs must be UNION, received $newSchema")
  //  }
}
