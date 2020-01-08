package com.sksamuel.avro4s
import com.sksamuel.avro4s.Codec.Typeclass
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

class TypeUnionCodec[T](ctx: SealedTrait[Typeclass, T]) extends Codec[T] {

  private val (codecByName, codecBySubtype) = {
    val subtypeCodecs = ctx.subtypes.map(st => new SubtypeCodec(st, ctx.annotations))
    (subtypeCodecs.map(c => c.fullName -> c).toMap, subtypeCodecs.map(c => c.st -> c).toMap)
  }

  val schema: Schema = {
    val sortedCodecs = {
      def priority(st: Subtype[Typeclass, T]): Float =
        new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
      codecBySubtype.values.toSeq.sortBy(c => (priority(c.st), c.st.typeName.full))
    }
    SchemaHelper.createSafeUnion(sortedCodecs.map(_.schema): _*)
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

class SubtypeCodec[T](val st: Subtype[Typeclass, T], annotations: Seq[Any]) {

  private val codec: Codec[st.SType] = if (annotations.nonEmpty) {
    st.typeclass match {
      case ac: AnnotableCodec[st.SType] => ac.withAnnotations(annotations)
      case c => c
    }
  } else st.typeclass

  val fullName = codec.schema.getFullName

  val schema = codec.schema

  def encodeSubtype(value: T): AnyRef = codec.encode(st.cast(value))

  def decodeSubtype(value: Any): T = codec.decode(value)
}
