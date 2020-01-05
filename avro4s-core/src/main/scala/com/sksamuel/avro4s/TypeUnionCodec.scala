package com.sksamuel.avro4s
import com.sksamuel.avro4s.Codec.Typeclass
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

class TypeUnionCodec[T](ctx: SealedTrait[Typeclass, T]) extends Codec[T] {

  private val sortedSubtypes = {
    def priority(st: Subtype[Typeclass, T]): Float = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    ctx.subtypes.sortBy(st => (priority(st), st.typeName.full))
  }

  private val subtypeFor = sortedSubtypes.map(st => st.typeclass.schema.getFullName -> st).toMap

  val schema: Schema = SchemaHelper.createSafeUnion(sortedSubtypes.map(_.typeclass.schema): _*)

  def encode(value: T): AnyRef = ctx.dispatch(value)(subtype => subtype.typeclass.encode(value.asInstanceOf[subtype.SType]))

  def decode(value: Any): T = value match {
    case container: GenericContainer =>
      val schemaName = container.getSchema.getFullName
      val subtype = subtypeFor.getOrElse(schemaName, sys.error(s"Could not find schema $schemaName in type union schemas ${subtypeFor.keys.toSeq.sorted.mkString("[", ", ", "]")}"))
      subtype.typeclass.decode(value)
    case _ => sys.error(s"Unsupported type $value in type union decoder")
  }
}
