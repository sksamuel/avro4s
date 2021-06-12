package com.sksamuel.avro4s.decoders

import com.sksamuel.avro4s.{Avro4sDecodingException, Decoder, Encoder}
import com.sksamuel.avro4s.avroutils.SchemaHelper
import com.sksamuel.avro4s.typeutils.{Annotations, Names, SubtypeOrdering}
import magnolia.SealedTrait
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

object TypeUnions {

  /**
    * Builds an [[Decoder]] for a sealed trait enum.
    */
  def decoder[T](ctx: SealedTrait[Decoder, T]): Decoder[T] = new Decoder[T] {
    override def decode(schema: Schema): Any => T = {
      require(schema.isUnion)

      val decodersByName = ctx.subtypes.map { st =>
        val names = Names(st.typeInfo)
        val subschema = SchemaHelper.extractTraitSubschema(names.fullName, schema)
        names.name -> st.typeclass.decode(subschema)
      }.toMap

      { value =>
        value match {
          case container: GenericContainer =>
            val schemaName = container.getSchema.getFullName
            decodersByName.get(schemaName).fold(
              {
                val schemaNames = decodersByName.keys.toSeq.sorted.mkString("[", ", ", "]")
                throw new Avro4sDecodingException(s"Could not find schema $schemaName in type union schemas $schemaNames", value)
              }
            )(_.apply(container))
          case _ => throw new Avro4sDecodingException(s"Unsupported type $value in type union decoder", value)
        }
      }
    }
  }
}