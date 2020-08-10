package com.sksamuel.avro4s
import com.sksamuel.avro4s.AvroValue.{AvroEnumSymbol, AvroString}
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.generic.GenericData
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

object ScalaEnums {

  def encoder[T](ctx: SealedTrait[Encoder, T]): Encoder[T] = create[Encoder, T](ctx, new EnumEncoder[T](_))

  def decoder[T](ctx: SealedTrait[Decoder, T]): Decoder[T] = create[Decoder, T](ctx, new EnumDecoder[T](_))

  private type Builder[Typeclass[_], T] = CodecData[Typeclass, T] => Typeclass[T]

  private def create[Typeclass[_], T](ctx: SealedTrait[Typeclass, T], builder: Builder[Typeclass, T]): Typeclass[T] = {
    val subtypes: Seq[Subtype[Typeclass, T]] = sortedSubtypes(ctx)
    val schema: Schema = buildSchema(ctx, subtypes)

    val symbolForSubtype: Map[Subtype[Typeclass, T], AnyRef] = subtypes.zipWithIndex.map {
      case (st, i) => st -> GenericData.get.createEnum(schema.getEnumSymbols.get(i), schema)
    }.toMap

    val runtimeMirror = universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
    val valueForSymbol: Map[String, T] =
      subtypes.zipWithIndex.map {
        case (st, i) =>
          val module = runtimeMirror.staticModule(st.typeName.full)
          val caseObject = runtimeMirror.reflectModule(module.asModule).instance.asInstanceOf[T]
          schema.getEnumSymbols.get(i) -> caseObject
      }.toMap

    val data =
      new CodecData[Typeclass, T](ctx, symbolForSubtype, valueForSymbol, SchemaFor[T](schema, DefaultFieldMapper))
    builder(data)
  }

  private class EnumDecoder[T](data: CodecData[Decoder, T]) extends Decoder[T] {
    val schemaFor: SchemaFor[T] = data.schemaFor

    import data._

    override def decode(value: AvroValue): T = value match {
      case AvroEnumSymbol(symbol) => valueForSymbol(symbol.toString)
      case AvroString(str) => valueForSymbol(str)
    }

    override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
      validateSchema(schemaFor, data.valueForSymbol)
      super.withSchema(schemaFor)
    }
  }

  private class EnumEncoder[T](data: CodecData[Encoder, T]) extends Encoder[T] {
    val schemaFor: SchemaFor[T] = data.schemaFor

    import data._

    def encode(value: T): AnyRef = ctx.dispatch(value)(symbolForSubtype)

    override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
      validateSchema(schemaFor, data.valueForSymbol)
      super.withSchema(schemaFor)
    }
  }

  private def validateSchema[T](schemaFor: SchemaFor[T], valueForSymbol: Map[String, T]): Unit = {
    val newSchema = schemaFor.schema
    if (newSchema.getType != Schema.Type.ENUM)
      throw new Avro4sConfigurationException(s"Schema type for enum codecs must be ENUM, received $newSchema")
    val currentSymbols = valueForSymbol.keys.toSet
    val newSymbols = newSchema.getEnumSymbols.asScala.toSet
    if (newSymbols != currentSymbols)
      throw new Avro4sConfigurationException(
        s"Enum codec symbols cannot be changed via schema; schema symbols are ${newSymbols.mkString(",")} - codec symbols are $currentSymbols")
  }

  private class CodecData[Typeclass[_], T](val ctx: SealedTrait[Typeclass, T],
                                           val symbolForSubtype: Map[Subtype[Typeclass, T], AnyRef],
                                           val valueForSymbol: Map[String, T],
                                           val schemaFor: SchemaFor[T])

  private def sortedSubtypes[TC[_], T](ctx: SealedTrait[TC, T]): Seq[Subtype[TC, T]] = {
    def priority(st: Subtype[TC, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    ctx.subtypes.sortWith((l, r) => priority(l) > priority(r))
  }

  def schema[Typeclass[_], T](ctx: SealedTrait[Typeclass, T]): Schema = buildSchema(ctx, sortedSubtypes(ctx))

  private def buildSchema[Typeclass[_], T](ctx: SealedTrait[Typeclass, T],
                                           sortedSubtypes: Seq[Subtype[Typeclass, T]]): Schema = {
    val symbols = sortedSubtypes.map { sub =>
      val nameExtractor = NameExtractor(sub.typeName, sub.annotations)
      nameExtractor.name
    }
    val nameExtractor = NameExtractor(ctx.typeName, ctx.annotations)

    val builder = SchemaBuilder.enumeration(nameExtractor.name).namespace(nameExtractor.namespace)

    val builderWithDefault = sealedTraitEnumDefaultValue(ctx) match {
      case Some(default) => builder.defaultSymbol(default)
      case None          => builder
    }

    builderWithDefault.symbols(symbols: _*)
  }

  private def sealedTraitEnumDefaultValue[TC[_], T](ctx: SealedTrait[TC, T]) = {
    val defaultExtractor = new AnnotationExtractors(ctx.annotations)
    defaultExtractor.enumDefault.flatMap { default =>
      ctx.subtypes.flatMap { st =>
        if (st.typeName.short == default.toString)
          Option(st.typeName.short)
        else
          None
      }.headOption
    }
  }
}
