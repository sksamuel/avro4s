package com.sksamuel.avro4s

import com.sksamuel.avro4s.Codec.Typeclass
import com.sksamuel.avro4s.ValueTypeCodec.UnderlyingCodec
import magnolia.{CaseClass, Param}
import org.apache.avro.{Schema, SchemaBuilder}

class ValueTypeCodec[T](ctx: CaseClass[Typeclass, T], val schema: Schema) extends Codec[T] with ChangeableSchemaCodec[T] {

  private val codec = new UnderlyingCodec(ctx.parameters.head, schema)

  def encode(value: T): AnyRef = codec.encodeUnderlying(value)

  def decode(value: Any): T = ctx.rawConstruct(List(codec.decodeUnderlying(value)))

  override def withSchema(schema: Schema): Typeclass[T] = new ValueTypeCodec(ctx, schema)
}

object ValueTypeCodec {
  def apply[T](ctx: CaseClass[Typeclass, T]): ValueTypeCodec[T] = {
    val schema = buildSchema(ctx)
    new ValueTypeCodec(ctx, schema)
  }

  def buildSchema[T](ctx: CaseClass[Typeclass, T]): Schema = {
    val annotations = new AnnotationExtractors(ctx.annotations)

    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)
    val namespace = nameExtractor.namespace
    val name = nameExtractor.name

    // if the class is a value type, then we need to use the schema for the single field inside the type
    // in other words, if we have `case class Foo(str:String)` then this just acts like a string
    // if we have a value type AND @AvroFixed is present on the class, then we simply return a schema of type fixed

    val param = ctx.parameters.head
    annotations.fixed match {
      case Some(size) =>
        val builder =
          SchemaBuilder.fixed(name).doc(annotations.doc.orNull).namespace(namespace).aliases(annotations.aliases: _*)
        annotations.props.foreach { case (k, v) => builder.prop(k, v) }
        builder.size(size)
      case None => param.typeclass.schema
    }
  }

  class UnderlyingCodec[T](val param: Param[Typeclass, T], schema: Schema) {

    // An encoder for a value type just needs to pass through the given value into an encoder
    // for the backing type. At runtime, the value type class won't exist, and the input
    // will be an instance of whatever the backing field of the value class was defined to be.
    // In other words, if you had a value type `case class Foo(str :String)` then the value
    // avro expects is a string, not a record of Foo, so the encoder for Foo should just encode
    // the underlying string

    private val codec: Typeclass[param.PType] = {
      val codec = param.typeclass
      codec match {
        case m: ChangeableSchemaCodec[param.PType] if m.schema != schema => m.withSchema(schema)
        case _                                                           => codec
      }
    }

    def encodeUnderlying(value: T): AnyRef = codec.encode(param.dereference(value))

    // In Scala, value types are erased at compile time. So in avro4s we assume that value types do not exist
    // in the avro side, neither in the schema, nor in the input values. Therefore, when creating a decoder
    // for a value type, the input will be a value of the underlying type. In other words, given a value type
    // `Foo(s: String) extends AnyVal` - the input will be a String, not a record containing a String
    // as it would be for a non-value type.

    // So the generated decoder for a value type should simply pass through to a generated decoder for
    // the underlying type without worrying about fields etc.

    def decodeUnderlying(value: Any): param.PType = codec.decode(value)

  }
}
