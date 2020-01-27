package com.sksamuel.avro4s
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.generic.{GenericData, GenericEnumSymbol}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.reflect.runtime.universe
import scala.collection.JavaConverters._

object ScalaEnums {

  def codec[T](ctx: SealedTrait[Codec, T]): Codec[T] = create(ctx, new EnumCodec[T](_))

  def encoder[T](ctx: SealedTrait[EncoderV2, T]): EncoderV2[T] = create(ctx, new EnumEncoder[T](_))

  def decoder[T](ctx: SealedTrait[DecoderV2, T]): DecoderV2[T] = create(ctx, new EnumDecoder[T](_))

  private type Builder[Typeclass[_], T] = CodecData[Typeclass, T] => Typeclass[T]

  private def create[Typeclass[_], T](ctx: SealedTrait[Typeclass, T], builder: Builder[Typeclass, T]): Typeclass[T] = {
    val subtypes: Seq[Subtype[Typeclass, T]] = sortedSubtypes(ctx)
    val schema: Schema = buildSchema(ctx, subtypes)

    val symbolForSubtype: Map[Subtype[Typeclass, T], AnyRef] = subtypes.zipWithIndex.map {
      case (st, i) => st -> GenericData.get.createEnum(schema.getEnumSymbols.get(i), schema)
    }.toMap

    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val valueForSymbol: Map[String, T] =
      subtypes.zipWithIndex.map {
        case (st, i) =>
          val module = runtimeMirror.staticModule(st.typeName.full)
          val caseObject = runtimeMirror.reflectModule(module.asModule).instance.asInstanceOf[T]
          schema.getEnumSymbols.get(i) -> caseObject
      }.toMap

    val data = new CodecData[Typeclass, T](ctx, symbolForSubtype, valueForSymbol, SchemaForV2[T](schema))
    builder(data)
  }

  private abstract class BaseCodec[Typeclass[_], T](data: CodecData[Typeclass, T]) extends SchemaAware[Typeclass, T] {

    val schemaFor = data.schemaFor

    import data._

    def encode(value: T): AnyRef = ctx.dispatch(value)(symbolForSubtype)

    def decode(value: Any): T = value match {
      case e: GenericEnumSymbol[_] => valueForSymbol(e.toString)
      case s: String               => valueForSymbol(s)
    }

    protected def validateSchema(schemaFor: SchemaForV2[T]): Unit = {
      val newSchema = schemaFor.schema
      require(newSchema.getType == Schema.Type.ENUM,
              s"Schema type for enum codecs must be ENUM, received ${newSchema.getType}")
      val currentSymbols = valueForSymbol.keys.toSet
      val newSymbols = newSchema.getEnumSymbols.asScala.toSet
      require(
        newSymbols == currentSymbols,
        s"Enum codec symbols cannot be changed via schema; schema symbols are ${newSymbols.mkString(",")} - codec symbols are $currentSymbols"
      )
    }
  }

  private class EnumCodec[T](data: CodecData[Codec, T]) extends BaseCodec[Codec, T](data) with Codec[T] {

    override def withSchema(schemaFor: SchemaForV2[T]): Codec[T] = {
      validateSchema(schemaFor)
      super.withSchema(schemaFor)
    }
  }

  private class EnumDecoder[T](data: CodecData[DecoderV2, T]) extends BaseCodec[DecoderV2, T](data) with DecoderV2[T] {

    override def withSchema(schemaFor: SchemaForV2[T]): DecoderV2[T] = {
      validateSchema(schemaFor)
      super.withSchema(schemaFor)
    }
  }

  private class EnumEncoder[T](data: CodecData[EncoderV2, T]) extends BaseCodec[EncoderV2, T](data) with EncoderV2[T] {

    override def withSchema(schemaFor: SchemaForV2[T]): EncoderV2[T] = {
      validateSchema(schemaFor)
      super.withSchema(schemaFor)
    }
  }

  private class CodecData[Typeclass[_], T](val ctx: SealedTrait[Typeclass, T],
                                           val symbolForSubtype: Map[Subtype[Typeclass, T], AnyRef],
                                           val valueForSymbol: Map[String, T],
                                           val schemaFor: SchemaForV2[T])

  private def sortedSubtypes[TC[_], T](ctx: SealedTrait[TC, T]): Seq[Subtype[TC, T]] = {
    def priority(st: Subtype[TC, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    ctx.subtypes.sortBy(st => (priority(st), st.typeName.full))
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
