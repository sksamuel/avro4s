package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime}
import java.util.UUID

import com.sksamuel.avro4s.SchemaUpdate.UseFieldMapper
import magnolia.{CaseClass, Magnolia, Param, SealedTrait}
import org.apache.avro.util.Utf8
import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}
import shapeless.{:+:, CNil, Coproduct}

import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * A [[SchemaFor]] generates an Avro Schema for a Scala or Java type.
  *
  * For example, a String SchemaFor could return an instance of Schema.Type.STRING
  * or Schema.Type.FIXED depending on the type required for Strings.
  */
final case class SchemaFor[T](schema: Schema, fieldMapper: FieldMapper = DefaultFieldMapper) extends Serializable {

  /**
    * Creates a SchemaFor[U] by applying a function Schema => Schema
    * to the schema generated by this instance.
    */
  def map[U](fn: Schema => Schema): SchemaFor[U] = SchemaFor[U](fn(schema), fieldMapper)

  def forType[U]: SchemaFor[U] = map[U](identity)
}


case class ScalePrecision(scale: Int, precision: Int)

object ScalePrecision {
  implicit val default = ScalePrecision(2, 8)
}


trait EnumSchemaFor {

  import scala.collection.JavaConverters._

  protected def addDefault[E](default: E)(schema: Schema): Schema = SchemaBuilder.
    enumeration(schema.getName).
    namespace(schema.getNamespace).
    defaultSymbol(default.toString).
    symbols(schema.getEnumSymbols.asScala.toList:_*)
}

object JavaEnumSchemaFor extends EnumSchemaFor {

  def apply[E <: Enum[_]](default: E)(implicit tag: ClassTag[E]): SchemaFor[E] = SchemaFor.javaEnumSchema.map[E](addDefault(default))
}

object ScalaEnumSchemaFor extends EnumSchemaFor {

  def apply[E <: scala.Enumeration#Value](default: E)(implicit tag: TypeTag[E]): SchemaFor[E] = SchemaFor.scalaEnumSchema.map[E](addDefault(default))
}

object SchemaFor {

  def apply[T](implicit schemaFor: SchemaFor[T]): SchemaFor[T] = schemaFor

  implicit val IntSchema: SchemaFor[Int] = SchemaFor[Int](SchemaBuilder.builder.intType)
  implicit val ByteSchema: SchemaFor[Byte] = IntSchema.forType
  implicit val ShortSchema: SchemaFor[Short] = IntSchema.forType
  implicit val LongSchema: SchemaFor[Long] = SchemaFor[Long](SchemaBuilder.builder.longType)
  implicit val FloatSchema: SchemaFor[Float] = SchemaFor[Float](SchemaBuilder.builder.floatType)
  implicit val DoubleSchema: SchemaFor[Double] = SchemaFor[Double](SchemaBuilder.builder.doubleType)
  implicit val BooleanSchema: SchemaFor[Boolean] = SchemaFor[Boolean](SchemaBuilder.builder.booleanType)
  implicit val ByteBufferSchema: SchemaFor[ByteBuffer] = SchemaFor[ByteBuffer](SchemaBuilder.builder.bytesType)
  implicit val CharSequenceSchema: SchemaFor[CharSequence] =
    SchemaFor[CharSequence](SchemaBuilder.builder.stringType)
  implicit val StringSchema: SchemaFor[String] = SchemaFor[String](SchemaBuilder.builder.stringType)
  implicit val Utf8Schema: SchemaFor[Utf8] = StringSchema.forType
  implicit val UUIDSchema: SchemaFor[UUID] = SchemaFor(LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType))

  implicit def javaEnumSchema[E <: Enum[_]](implicit tag: ClassTag[E]): SchemaFor[E] = {
    val typeInfo = TypeInfo.fromClass(tag.runtimeClass)
    val nameExtractor = NameExtractor(typeInfo)
    val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)
    val schema = SchemaBuilder.enumeration(nameExtractor.name).namespace(nameExtractor.namespace).symbols(symbols: _*)
    SchemaFor[E](schema)
  }

  implicit def scalaEnumSchema[E <: scala.Enumeration#Value](implicit tag: TypeTag[E]): SchemaFor[E] = {
    val typeRef = tag.tpe match {
      case t @ TypeRef(_, _, _) => t
    }

    val valueType = typeOf[E]
    val pre = typeRef.pre.typeSymbol.typeSignature.members.sorted
    val syms = pre
      .filter { sym =>
        !sym.isMethod &&
        !sym.isType &&
        sym.typeSignature.baseType(valueType.typeSymbol) =:= valueType
      }
      .map { sym =>
        sym.name.decodedName.toString.trim
      }

    val as = typeRef.pre.typeSymbol.annotations
    val nameAnnotation = as.collectFirst {
      case a: AvroName => a.name
    }
    val namespaceAnnotation = as.collectFirst {
      case a: AvroNamespace => a.namespace
    }
    val props = as.collect {
      case prop: AvroProp => prop.key -> prop.value
    }

    val nameExtractor = NameExtractor(TypeInfo.fromType(typeRef.pre))

    val s = SchemaBuilder.enumeration(nameExtractor.name).namespace(nameExtractor.namespace).symbols(syms: _*)
    props.foreach {
      case (key, value) =>
        s.addProp(key, value)
    }
    SchemaFor[E](s)
  }

  object TimestampNanosLogicalType extends LogicalType("timestamp-nanos") {
    override def validate(schema: Schema): Unit = {
      super.validate(schema)
      if (schema.getType != Schema.Type.LONG) {
        throw new IllegalArgumentException("Logical type timestamp-nanos must be backed by long")
      }
    }
  }

  object OffsetDateTimeLogicalType extends LogicalType("datetime-with-offset") {
    override def validate(schema: Schema): Unit = {
      super.validate(schema)
      if (schema.getType != Schema.Type.STRING) {
        throw new IllegalArgumentException("Logical type iso-datetime with offset must be backed by String")
      }
    }
  }

  implicit val InstantSchema: SchemaFor[Instant] =
    SchemaFor[Instant](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))
  implicit val DateSchema: SchemaFor[Date] = SchemaFor(
    LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType))
  implicit val LocalDateSchema: SchemaFor[LocalDate] = DateSchema.forType
  implicit val LocalDateTimeSchema: SchemaFor[LocalDateTime] = SchemaFor(
    TimestampNanosLogicalType.addToSchema(SchemaBuilder.builder.longType))
  implicit val OffsetDateTimeSchema: SchemaFor[OffsetDateTime] = SchemaFor(
    OffsetDateTimeLogicalType.addToSchema(SchemaBuilder.builder.stringType))
  implicit val LocalTimeSchema: SchemaFor[LocalTime] = SchemaFor(
    LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder.longType))
  implicit val TimestampSchema: SchemaFor[Timestamp] =
    SchemaFor[Timestamp](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))

  implicit val noneSchema: SchemaFor[None.type] = SchemaFor[None.type](SchemaBuilder.builder.nullType)

  implicit def optionSchema[T](implicit schemaForItem: SchemaFor[T]): SchemaFor[Option[T]] = {
    schemaForItem.map[Option[T]](itemSchema =>
      SchemaHelper.createSafeUnion(itemSchema, SchemaBuilder.builder().nullType()))
  }

  implicit def eitherSchema[A, B](implicit leftFor: SchemaFor[A],
                                  rightFor: SchemaFor[B]): SchemaFor[Either[A, B]] =
    SchemaFor[Either[A, B]](SchemaHelper.createSafeUnion(leftFor.schema, rightFor.schema))

  implicit val ByteArraySchema: SchemaFor[Array[Byte]] = SchemaFor[Array[Byte]](SchemaBuilder.builder.bytesType)
  implicit val ByteListSchema: SchemaFor[List[Byte]] = ByteArraySchema.forType
  implicit val ByteSeqSchema: SchemaFor[Seq[Byte]] = ByteArraySchema.forType
  implicit val ByteVectorSchema: SchemaFor[Vector[Byte]] = ByteArraySchema.forType

  private def _iterableSchema[C[X] <: Iterable[X], T](implicit schemaForItem: SchemaFor[T]): SchemaFor[C[T]] =
    SchemaFor[C[T]](SchemaBuilder.array.items(schemaForItem.schema), schemaForItem.fieldMapper)

  implicit def arraySchema[T](implicit schemaForItem: SchemaFor[T]): SchemaFor[Array[T]] =
    _iterableSchema(schemaForItem).forType
  implicit def iterableSchema[T](implicit schemaForItem: SchemaFor[T]): SchemaFor[Iterable[T]] =
    _iterableSchema(schemaForItem).forType
  implicit def listSchema[T](implicit schemaForItem: SchemaFor[T]): SchemaFor[List[T]] =
    _iterableSchema(schemaForItem).forType
  implicit def setSchema[T](implicit schemaForItem: SchemaFor[T]): SchemaFor[Set[T]] =
    _iterableSchema(schemaForItem).forType
  implicit def vectorSchema[T](implicit schemaForItem: SchemaFor[T]): SchemaFor[Vector[T]] =
    _iterableSchema(schemaForItem).forType
  implicit def seqSchema[T](implicit schemaForItem: SchemaFor[T]): SchemaFor[Seq[T]] =
    _iterableSchema(schemaForItem).forType

  implicit def mapSchema[T](implicit schemaForValue: SchemaFor[T]): SchemaFor[Map[String, T]] =
    SchemaFor(SchemaBuilder.map().values(schemaForValue.schema), schemaForValue.fieldMapper)

  implicit def bigDecimalSchema(implicit sp: ScalePrecision = ScalePrecision.default): SchemaFor[BigDecimal] =
    SchemaFor(LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType))

  implicit def singleElementSchema[H](implicit schemaFor: SchemaFor[H]): SchemaFor[H :+: CNil] =
    SchemaFor(SchemaHelper.createSafeUnion(schemaFor.schema), schemaFor.fieldMapper)

  implicit def coproductSchema[H, T <: Coproduct](implicit schemaForH: SchemaFor[H],
                                                  schemaForT: SchemaFor[T]): SchemaFor[H :+: T] =
    SchemaFor(SchemaHelper.createSafeUnion(schemaForH.schema, schemaForT.schema), schemaForH.fieldMapper)

  implicit def tuple2SchemaFor[A, B](implicit a: SchemaFor[A], b: SchemaFor[B]): SchemaFor[(A, B)] = {
    SchemaFor(SchemaBuilder.record("Tuple2").namespace("scala").doc(null)
        .fields()
        .name("_1").`type`(a.schema).noDefault()
        .name("_2").`type`(b.schema).noDefault()
        .endRecord(),
      a.fieldMapper
    )
  }

  implicit def tuple3SchemaFor[A, B, C](implicit a: SchemaFor[A],
                                        b: SchemaFor[B],
                                        c: SchemaFor[C]): SchemaFor[(A, B, C)] = {
    SchemaFor(SchemaBuilder.record("Tuple3").namespace("scala").doc(null)
      .fields()
      .name("_1").`type`(a.schema).noDefault()
      .name("_2").`type`(b.schema).noDefault()
      .name("_3").`type`(c.schema).noDefault()
      .endRecord(),
      a.fieldMapper
    )
  }

  implicit def tuple4SchemaFor[A, B, C, D](implicit a: SchemaFor[A],
                                           b: SchemaFor[B],
                                           c: SchemaFor[C],
                                           d: SchemaFor[D]): SchemaFor[(A, B, C, D)] = {
    SchemaFor(SchemaBuilder.record("Tuple4").namespace("scala").doc(null)
      .fields()
      .name("_1").`type`(a.schema).noDefault()
      .name("_2").`type`(b.schema).noDefault()
      .name("_3").`type`(c.schema).noDefault()
      .name("_4").`type`(d.schema).noDefault()
      .endRecord(),
      a.fieldMapper
    )
  }

  implicit def tuple5SchemaFor[A, B, C, D, E](implicit a: SchemaFor[A],
                                              b: SchemaFor[B],
                                              c: SchemaFor[C],
                                              d: SchemaFor[D],
                                              e: SchemaFor[E]): SchemaFor[(A, B, C, D, E)] = {
    SchemaFor(SchemaBuilder.record("Tuple5").namespace("scala").doc(null)
      .fields()
      .name("_1").`type`(a.schema).noDefault()
      .name("_2").`type`(b.schema).noDefault()
      .name("_3").`type`(c.schema).noDefault()
      .name("_4").`type`(d.schema).noDefault()
      .name("_5").`type`(e.schema).noDefault()
      .endRecord(),
      a.fieldMapper
    )
  }

  type Typeclass[T] = SchemaFor[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): SchemaFor[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.schema(ctx, UseFieldMapper(fieldMapper))
      case SealedTraitShape.ScalaEnum => SchemaFor[T](ScalaEnums.schema(ctx), fieldMapper)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): SchemaFor[T] = {
    val paramSchema = (p: Param[Typeclass, T]) => p.typeclass.schema

    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record =>
        Records.schema(ctx, fieldMapper, None, paramSchema)
      case CaseClassShape.ValueType =>
        SchemaFor[T](ValueTypes.buildSchema(ctx, None, paramSchema), fieldMapper)
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}
