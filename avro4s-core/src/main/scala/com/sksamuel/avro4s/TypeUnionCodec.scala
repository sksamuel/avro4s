package com.sksamuel.avro4s
import com.sksamuel.avro4s
import com.sksamuel.avro4s.Codec.Typeclass
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

class TypeUnionCodec[T](ctx: SealedTrait[Typeclass, T],
                        val schema: Schema,
                        codecByName: Map[String, UnionEntryCodec[T]],
                        codecBySubtype: Map[Subtype[Typeclass, T], UnionEntryCodec[T]])
    extends Codec[T]
    with ModifiableNamespaceCodec[T] {

  def withNamespace(namespace: String): Codec[T] = TypeUnionCodec(ctx, Some(namespace))

  override def withSchema(schemaFor: SchemaForV2[T], fieldMapper: FieldMapper): Typeclass[T] = {
    val newSchema = schemaFor.schema
    require(newSchema.getType == Schema.Type.UNION,
            s"Schema type for record codecs must be UNION, received ${newSchema.getType}")
    TypeUnionCodec(ctx, schemaFor, fieldMapper)
  }

  def encode(value: T): AnyRef = ctx.dispatch(value)(subtype => codecBySubtype(subtype).encodeSubtype(value))

  def decode(value: Any): T = value match {
    case container: GenericContainer =>
      val schemaName = container.getSchema.getFullName
      val codec = codecByName.getOrElse(
        schemaName,
        sys.error(
          s"Could not find schema $schemaName in type union schemas ${codecByName.keys.toSeq.sorted.mkString("[", ", ", "]")}"))
      codec.decodeSubtype(value)
    case _ => sys.error(s"Unsupported type $value in type union decoder")
  }
}

object TypeUnionCodec {
  def apply[T](ctx: SealedTrait[Typeclass, T], namespaceOverride: Option[String] = None): TypeUnionCodec[T] = {
    val subtypeCodecs: Seq[UnionEntryCodec[T]] = ctx.subtypes.map { st =>
      val ns = namespaceOverride.orElse(new AnnotationExtractors(st.annotations).namespace)
      new UnionEntryCodec(st, namespace = ns)
    }
    val schema = buildSchema(ctx, (s: Subtype[Typeclass, T]) => s.typeclass.schema)
    apply(ctx, subtypeCodecs, schema)
  }

  def apply[T](ctx: SealedTrait[Typeclass, T],
               schemaFor: SchemaForV2[T],
               fieldMapper: FieldMapper): TypeUnionCodec[T] = {
    val schema = schemaFor.schema

    val subtypeCodecs: Seq[UnionEntryCodec[T]] = ctx.subtypes.map { st =>
      val nameExtractor = NameExtractor(st.typeName, st.annotations ++ ctx.annotations)
      val subtraitSchema = SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema)
      new UnionEntryCodec(st, schemaOverride = Some(subtraitSchema), fieldMapper = fieldMapper)
    }
    apply(ctx, subtypeCodecs, schema)

  }

  private def apply[T](ctx: SealedTrait[avro4s.Codec.Typeclass, T],
                       subtypeCodecs: Seq[UnionEntryCodec[T]],
                       schema: Schema) = {
    val codecByName: Map[String, UnionEntryCodec[T]] = subtypeCodecs.map(c => c.fullName -> c).toMap
    val codecBySubtype: Map[Subtype[Typeclass, T], UnionEntryCodec[T]] = subtypeCodecs.map(c => c.st -> c).toMap
    new TypeUnionCodec(ctx, schema, codecByName, codecBySubtype)
  }

  def buildSchema[TC[_], T](ctx: SealedTrait[TC, T], subtypeSchema: Subtype[TC, T] => Schema): Schema = {
    val sortedSubtypes = {
      def priority(st: Subtype[TC, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
      ctx.subtypes.sortBy(st => (priority(st), st.typeName.full))
    }
    SchemaHelper.createSafeUnion(sortedSubtypes.map(subtypeSchema): _*)
  }
}

class UnionEntryCodec[T](val st: Subtype[Typeclass, T],
                         namespace: Option[String] = None,
                         schemaOverride: Option[Schema] = None,
                         fieldMapper: FieldMapper = DefaultFieldMapper) {

  private val codec: Codec[st.SType] = {
    (st.typeclass, namespace, schemaOverride) match {
      case (codec, _, Some(s)) =>
        codec.withSchema(SchemaForV2[st.SType](s), fieldMapper)
      case (mnc: ModifiableNamespaceCodec[st.SType] @unchecked, Some(n), _) => mnc.withNamespace(n)
      case (codec, _, _)                                                    => codec
    }
  }

  val fullName = codec.schema.getFullName

  val schema = codec.schema

  def encodeSubtype(value: T): AnyRef = codec.encode(st.cast(value))

  def decodeSubtype(value: Any): T = codec.decode(value)
}
