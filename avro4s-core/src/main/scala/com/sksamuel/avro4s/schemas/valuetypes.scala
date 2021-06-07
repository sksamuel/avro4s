package com.sksamuel.avro4s.schemas

import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.typeutils.{Annotations, Names}
import magnolia.CaseClass
import org.apache.avro.SchemaBuilder
import org.apache.avro.Schema

object ValueTypes {

  /**
    * Builds a schema for a value type.
    */
  def schema[T](ctx: CaseClass[SchemaFor, T]): Schema =
    val annos: Annotations = Annotations(ctx) // taking over @AvroFixed and the like
    val names = Names(ctx.typeInfo, annos, ctx.typeAnnotations)

    // if the class is a value type, then we need to use the schema for the single field inside the type
    // in other words, if we have `case class Foo(str: String) extends AnyVal` then this acts just like String.
    // if we have a value type AND @AvroFixed is present on the class, then we simply return a schema of type fixed
    annos.fixed match {
      case Some(size) =>
        val builder =
          SchemaBuilder
            .fixed(names.name)
            .doc(annos.doc.orNull)
            .namespace(names.namespace)
            .aliases(annos.aliases: _*)
        annos.props.foreach { case (k, v) => builder.prop(k, v) }
        builder.size(size)
      case None =>
        ctx.params.head.typeclass.schema
    }
}