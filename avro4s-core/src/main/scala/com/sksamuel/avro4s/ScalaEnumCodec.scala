package com.sksamuel.avro4s
import com.sksamuel.avro4s.Codec.Typeclass
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.generic.{GenericData, GenericEnumSymbol}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.reflect.runtime.universe
import scala.collection.JavaConverters._

class ScalaEnumCodec[T](ctx: SealedTrait[Typeclass, T],
                        symbolForSubtype: Map[Subtype[Typeclass, T], AnyRef],
                        valueForSymbol: Map[String, T],
                        val schema: Schema)
    extends Codec[T] {

  def encode(value: T): AnyRef = ctx.dispatch(value)(symbolForSubtype)

  def decode(value: Any): T = value match {
    case e: GenericEnumSymbol[_] => valueForSymbol(e.toString)
    case s: String               => valueForSymbol(s)
  }

  override def withSchema(schemaFor: SchemaForV2[T]): Typeclass[T] = {
    val newSchema = schemaFor.schema
    require(newSchema.getType == Schema.Type.ENUM, s"Schema type for enum codecs must be ENUM, received ${newSchema.getType}")
    val currentSymbols = valueForSymbol.keys.toSet
    val newSymbols = newSchema.getEnumSymbols.asScala.toSet
    require(newSymbols == currentSymbols, s"Enum codec symbols cannot be changed via schema; schema symbols are ${newSymbols.mkString(",")} - codec symbols are ${currentSymbols}")
    super.withSchema(schemaFor)
  }
}

object ScalaEnumCodec {

  def apply[T](ctx: SealedTrait[Typeclass, T]) = {
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

    new ScalaEnumCodec(ctx, symbolForSubtype, valueForSymbol, schema)
  }

  def sortedSubtypes[TC[_], T](ctx: SealedTrait[TC, T]): Seq[Subtype[TC, T]] = {
    def priority(st: Subtype[TC, T]) = new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
    ctx.subtypes.sortBy(st => (priority(st), st.typeName.full))
  }

  def buildSchema[TC[_], T](ctx: SealedTrait[TC, T]): Schema = buildSchema(ctx, sortedSubtypes(ctx))

  def buildSchema[TC[_], T](ctx: SealedTrait[TC, T], sortedSubtypes: Seq[Subtype[TC, T]]): Schema = {
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

class ScalaEnumEntryCodec[T](val st: Subtype[Typeclass, T]) {}
