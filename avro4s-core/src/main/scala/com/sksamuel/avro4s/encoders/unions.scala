package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.avroutils.SchemaHelper
import com.sksamuel.avro4s.typeutils.SubtypeOrdering
import com.sksamuel.avro4s.typeutils.{Annotations, Names}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import magnolia1.SealedTrait
import org.apache.avro.Schema

object TypeUnions {

  /**
    * Builds an [[Encoder]] for a sealed trait enum.
    */
  def encoder[T](ctx: SealedTrait[Encoder, T]): Encoder[T] = new Encoder[T] {
    override def encode(schema: Schema): T => AnyRef = {
      require(schema.isUnion)

      val encoderBySubtype = ctx.subtypes.sorted(SubtypeOrdering).map(st => {

        val annos: Annotations = new Annotations(st.annotations, st.inheritedAnnotations)
        val names = Names(st.typeInfo, annos)

        val subschema: Schema = SchemaHelper.extractTraitSubschema(names.fullName, schema)
        val encodeT: T => AnyRef = st.typeclass.asInstanceOf[Encoder[T]].encode(subschema)

        (st, encodeT)
      }).toMap

      { value =>
        ctx.choose(value) { st => encoderBySubtype(st.subtype).apply(st.cast(value)) }
      }
    }
  }
}
