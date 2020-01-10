package com.sksamuel.avro4s
import com.sksamuel.avro4s.Codec.Typeclass
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

class TypeUnionCodec[T](ctx: SealedTrait[Typeclass, T],
                        val schema: Schema,
                        codecByName: Map[String, UnionEntryCodec[T]],
                        codecBySubtype: Map[Subtype[Typeclass, T], UnionEntryCodec[T]])
    extends Codec[T] {

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

  override def withSchema(schema: Schema): Typeclass[T] = {
    // implementing this method could be really tedious, as it's not clear how to propagate the new schema to the union entry codecs.
    throw new UnsupportedOperationException("Extending type union codecs with a custom schema isn't supported")
  }
}

object TypeUnionCodec {
  def apply[T](ctx: SealedTrait[Typeclass, T]): TypeUnionCodec[T] = {
    val subtypeCodecs: Seq[UnionEntryCodec[T]] = ctx.subtypes.map(st => new UnionEntryCodec(st, ctx.annotations))
    val schema = buildSchema(subtypeCodecs)
    val codecByName: Map[String, UnionEntryCodec[T]] = subtypeCodecs.map(c => c.fullName -> c).toMap
    val codecBySubtype: Map[Subtype[Typeclass, T], UnionEntryCodec[T]] = subtypeCodecs.map(c => c.st -> c).toMap
    new TypeUnionCodec(ctx, schema, codecByName, codecBySubtype)
  }

  def buildSchema[T](codecs: Seq[UnionEntryCodec[T]]): Schema = {
    val sortedCodecs = {
      def priority(st: Subtype[Typeclass, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
      codecs.sortBy(c => (priority(c.st), c.st.typeName.full))
    }
    SchemaHelper.createSafeUnion(sortedCodecs.map(_.schema): _*)
  }
}

class UnionEntryCodec[T](val st: Subtype[Typeclass, T], annotations: Seq[Any]) {

  private val codec: Codec[st.SType] = if (annotations.nonEmpty) {
    st.typeclass match {
      case ac: AnnotableCodec[st.SType] => ac.withAnnotations(annotations)
      case c                            => c
    }
  } else st.typeclass

  val fullName = codec.schema.getFullName

  val schema = codec.schema

  def encodeSubtype(value: T): AnyRef = codec.encode(st.cast(value))

  def decodeSubtype(value: Any): T = codec.decode(value)
}
