package com.sksamuel.avro4s
import com.sksamuel.avro4s.Codec.Typeclass
import magnolia.{SealedTrait, Subtype}
import org.apache.avro.generic.{GenericData, GenericEnumSymbol}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

class ScalaEnumCodec[T](ctx: SealedTrait[Typeclass, T],
                        symbolForSubtype: Map[Subtype[Typeclass, T], AnyRef],
                        valueForSymbol: Map[String, T],
                        val schema: Schema)
    extends Codec[T] {

  override def withSchema(schema: Schema): Typeclass[T] = {
    require(schema.getType == Schema.Type.ENUM)
    require(schema.getEnumSymbols.size == ctx.subtypes.size)

    // matching the new schema to the old could also be done via positioning of the enum symbols.
    // this here assumes we match it by symbol name.
    val symbols = schema.getEnumSymbols.asScala
    require(valueForSymbol.keys.forall(symbols.contains), "enum symbols must remain the same")

    val newSymbolForSubtype = symbols.map { symbol =>
      val st = symbolForSubtype.find(entry => entry._2.toString == symbol).get._1
      st -> GenericData.get.createEnum(symbol, schema)
    }.toMap

    new ScalaEnumCodec(ctx, newSymbolForSubtype, valueForSymbol, schema)
  }

  def encode(value: T): AnyRef = ctx.dispatch(value)(symbolForSubtype)

  def decode(value: Any): T = value match {
    case e: GenericEnumSymbol[_] => valueForSymbol(e.toString)
    case s: String               => valueForSymbol(s)
  }
}

object ScalaEnumCodec {

  def apply[T](ctx: SealedTrait[Typeclass, T]) = {
    val sortedSubtypes: Seq[Subtype[Typeclass, T]] = {
      def priority(st: Subtype[Typeclass, T]): Float =
        new AnnotationExtractors(st.annotations).sortPriority.getOrElse(0.0f)
      ctx.subtypes.sortBy(st => (priority(st), st.typeName.full))
    }

    val schema: Schema = buildSchema(ctx, sortedSubtypes)

    val symbolForSubtype: Map[Subtype[Typeclass, T], AnyRef] = sortedSubtypes.zipWithIndex.map {
      case (st, i) => st -> GenericData.get.createEnum(schema.getEnumSymbols.get(i), schema)
    }.toMap

    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val valueForSymbol: Map[String, T] =
      sortedSubtypes.zipWithIndex.map {
        case (st, i) =>
          val module = runtimeMirror.staticModule(st.typeName.full)
          val caseObject = runtimeMirror.reflectModule(module.asModule).instance.asInstanceOf[T]
          schema.getEnumSymbols.get(i) -> caseObject
      }.toMap

    new ScalaEnumCodec(ctx, symbolForSubtype, valueForSymbol, schema)
  }

  def buildSchema[T](ctx: SealedTrait[Typeclass, T], sortedSubtypes: Seq[Subtype[Typeclass, T]]): Schema = {
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

  private def sealedTraitEnumDefaultValue[T](ctx: SealedTrait[Typeclass, T]) = {
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
