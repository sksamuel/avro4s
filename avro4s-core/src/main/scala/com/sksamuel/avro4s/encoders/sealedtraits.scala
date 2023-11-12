package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.typeutils.{Annotations, Names, EnumOrdering}
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import magnolia1.SealedTrait
import org.apache.avro.generic.GenericData
import org.apache.avro.{Schema, SchemaBuilder}

object SealedTraits {
  def encoder[T](ctx: SealedTrait[Encoder, T]): Encoder[T] = new Encoder[T] {
    override def encode(schema: Schema): T => Any = {
      val symbolForSubtype: Map[SealedTrait.Subtype[Encoder, T, _], AnyRef] = ctx.subtypes.sortBy(_.index).sorted(EnumOrdering).zipWithIndex.map {
        case (st, i) => st -> GenericData.get.createEnum(schema.getEnumSymbols.get(i), schema)
      }.toMap
      { (value: T) => ctx.choose(value) { st => symbolForSubtype(st.subtype) } }
    }
  }
}