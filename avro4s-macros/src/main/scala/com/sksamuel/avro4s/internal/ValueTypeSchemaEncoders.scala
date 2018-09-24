package com.sksamuel.avro4s.internal

import com.sksamuel.avro4s.internal.SchemaEncoder.create
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.util.{Failure, Success, Try}

trait ValueTypeSchemaEncoders {

  import scala.collection.JavaConverters._

  // union schemas can't contain other union schemas as a direct
  // child, so whenever we create a union, we need to check if our
  // children are unions
  // if they are, we just merge them into the union we're creating
  def extractUnionSchemas(schema: Schema): Seq[Schema] = Try(schema.getTypes /* throws an error if we're not a union */) match {
    case Success(subschemas) => subschemas.asScala
    case Failure(_) => Seq(schema)
  }

  // for a union the type that has a default must be first,
  // if there is no default, then null is first by convention
  def moveNullToHead(schemas: Seq[Schema]) = {
    val (nulls, withoutNull) = schemas.partition(_.getType == Schema.Type.NULL)
    nulls.headOption.toSeq ++ withoutNull
  }

  implicit object NullableSchemaEncoder extends SchemaEncoder[NullableType] {
    override def encode(dataType: NullableType): Schema = {
      val flattened = extractUnionSchemas(create(dataType.elementType))
      val schemas = Schema.create(Schema.Type.NULL) +: flattened
      Schema.createUnion(moveNullToHead(schemas).asJava)
    }
  }

  implicit object StringSchemaEncoder extends SchemaEncoder[StringType.type] {
    private val schema = Schema.create(Schema.Type.STRING)
    override def encode(dataType: StringType.type): Schema = schema
  }

  implicit object BooleanSchemaEncoder extends SchemaEncoder[BooleanType.type] {
    private val schema = Schema.create(Schema.Type.BOOLEAN)
    override def encode(dataType: BooleanType.type): Schema = schema
  }

  implicit object TimestampTypeSchemaEncoder extends SchemaEncoder[TimestampType.type] {
    private val schema = Schema.create(Schema.Type.LONG)
    LogicalTypes.timestampMillis().addToSchema(schema)
    override def encode(dataType: TimestampType.type): Schema = schema
  }

  implicit object LocalTimeTypeSchemaEncoder extends SchemaEncoder[LocalTimeType.type] {
    private val schema = Schema.create(Schema.Type.INT)
    LogicalTypes.timeMillis().addToSchema(schema)
    override def encode(dataType: LocalTimeType.type): Schema = schema
  }

  implicit object LocalDateTimeTypeSchemaEncoder extends SchemaEncoder[LocalDateTimeType.type] {
    private val schema = Schema.create(Schema.Type.LONG)
    LogicalTypes.timestampMillis().addToSchema(schema)
    override def encode(dataType: LocalDateTimeType.type): Schema = schema
  }

  implicit object ByteTypeSchemaEncoder extends SchemaEncoder[ByteType.type] {
    private val schema = Schema.create(Schema.Type.INT)
    override def encode(dataType: ByteType.type): Schema = schema
  }

  implicit object DoubleTypeSchemaEncoder extends SchemaEncoder[DoubleType.type] {
    private val schema = Schema.create(Schema.Type.DOUBLE)
    override def encode(dataType: DoubleType.type): Schema = schema
  }

  implicit object FloatTypeSchemaEncoder extends SchemaEncoder[FloatType.type] {
    private val schema = Schema.create(Schema.Type.FLOAT)
    override def encode(dataType: FloatType.type): Schema = schema
  }

  implicit object FixedTypeSchemaEncoder extends SchemaEncoder[FixedType] {
    override def encode(fixed: FixedType): Schema =
      Schema.createFixed(fixed.name, null, fixed.namespace.orNull, fixed.size)
  }

  implicit object ShortTypeSchemaEncoder extends SchemaEncoder[ShortType.type] {
    private val schema = Schema.create(Schema.Type.INT)
    override def encode(dataType: ShortType.type): Schema = schema
  }

  implicit object IntTypeSchemaEncoder extends SchemaEncoder[IntType.type] {
    private val schema = Schema.create(Schema.Type.INT)
    override def encode(dataType: IntType.type): Schema = schema
  }

  implicit object LongTypeSchemaEncoder extends SchemaEncoder[LongType.type] {
    private val schema = Schema.create(Schema.Type.LONG)
    override def encode(dataType: LongType.type): Schema = schema
  }

  implicit object BinaryTypeSchemaEncoder extends SchemaEncoder[BinaryType.type] {
    // binary type can be handled in two ways, either bytes or fixed
    // to return a field with fixed, either use @AvroFixed or provide a custom
    // typeclass that returns FixedType
    private val schema = Schema.create(Schema.Type.BYTES)
    override def encode(dataType: BinaryType.type): Schema = schema
  }

  implicit object EnumTypeSchemaEncoder extends SchemaEncoder[EnumType] {
    override def encode(enumType: EnumType): Schema = {
      val extractor = new AnnotationExtractors(enumType.annotations)
      val namespace = extractor.namespace.getOrElse(enumType.packageName)
      SchemaBuilder.enumeration(enumType.simpleName).namespace(namespace).symbols(enumType.symbols: _*)
    }
  }

  implicit object DecimalTypeSchemaEncoder extends SchemaEncoder[DecimalType] {
    override def encode(decimal: DecimalType): Schema = {
      val schema = Schema.create(Schema.Type.BYTES)
      LogicalTypes.decimal(decimal.precision, decimal.scale).addToSchema(schema)
    }
  }

  implicit object MapTypeSchemaEncoder extends SchemaEncoder[MapType] {
    override def encode(map: MapType): Schema = Schema.createMap(create(map.valueType))
  }

  implicit object UnionTypeSchemaEncoder extends SchemaEncoder[UnionType] {
    override def encode(union: UnionType): Schema = {
      val schemas = union.types.map(create).flatMap(extractUnionSchemas)
      Schema.createUnion(moveNullToHead(schemas).asJava)
    }
  }

  implicit object LocalDateTypeSchemaEncoder extends SchemaEncoder[LocalDateType.type] {
    private val schema = Schema.create(Schema.Type.INT)
    LogicalTypes.date().addToSchema(schema)
    override def encode(dataType: LocalDateType.type): Schema = schema
  }

  implicit object ArrayTypeSchemaEncoder extends SchemaEncoder[ArrayType] {
    override def encode(array: ArrayType): Schema = SchemaBuilder.array().items(create(array.valueType))
  }

  implicit object UUIDTypeSchemaEncoder extends SchemaEncoder[UUIDType.type] {
    private val schema = Schema.create(Schema.Type.STRING)
    LogicalTypes.uuid().addToSchema(schema)
    override def encode(dataType: UUIDType.type): Schema = schema
  }
}
