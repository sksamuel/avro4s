package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.{AvroName, SchemaFor}
import com.sksamuel.avro4s.typeutils.{Annotations, Names, EnumOrdering}
import magnolia1.SealedTrait
import org.apache.avro.{Schema, SchemaBuilder}

object SealedTraits {
  def schema[T](ctx: SealedTrait[SchemaFor, T]): Schema = {

    val names = Names(ctx.typeInfo, Annotations(ctx.annotations))

    // It currently appears (magnolia1, v 1) that all the enum elements carry the same annotation set
    // as the enumeration symbol itself, which leads to name clash in the generated schema. 
    // For now I'm disabling AvroName annotation interpretation of the symbols
    // if its name equals to the whole enumeration name.
    // Annotaions that are attached to the enum elements are not visible here.
    // Looks lilke we ned to have a look into either Magnolia or Scala 3.
    val symbols = ctx.subtypes.sorted(EnumOrdering.reverse).map { st =>
      Names(
        st.typeInfo,
        Annotations(
          st.annotations.filter {
            case an: AvroName => an.name != names.name
            case _ => true
          }
        )
      ).name
    }

    SchemaBuilder.enumeration(names.name).namespace(names.namespace).symbols(symbols*)

    // todo once magnolia supports scala 3 defaults
    //    val builderWithDefault = sealedTraitEnumDefaultValue(ctx) match {
    //      case Some(default) => builder.defaultSymbol(default)
    //      case None          => builder
    //    }
    //
  }
}