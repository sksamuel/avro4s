package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{ Decoder, Avro4sConfigurationException }
import org.apache.avro.Schema
import com.sksamuel.avro4s.avroutils.SchemaHelper
import com.sksamuel.avro4s.typeutils.{Annotations, Names}
import org.apache.avro.generic.GenericData
import magnolia1.SealedTrait

object SealedTraits {

  def decoder[T](ctx: magnolia1.SealedTrait[Decoder, T]): Decoder[T] = new Decoder[T] {
    override def decode(schema: Schema): Any => T = {
      require(schema.getType == Schema.Type.ENUM)
      val typeForSymbol: Map[GenericData.EnumSymbol, SealedTrait.Subtype[Decoder, T, _]] =
        ctx.subtypes.zipWithIndex.map { (st, i) =>
          val enumSymbol = GenericData.get.createEnum(schema.getEnumSymbols.get(i), schema).asInstanceOf[GenericData.EnumSymbol]
          enumSymbol -> st
        }.toMap

      { case value: GenericData.EnumSymbol =>
        val symtype = typeForSymbol(value)
        val tc = symtype.typeclass
        tc.decode(value.getSchema)(value)
      }
    }
  }
}
