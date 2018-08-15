package com.sksamuel.avro4s.schema

import java.io.Serializable

import com.sksamuel.avro4s.internal._
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

trait SchemaEncoder[A] extends Serializable {
  def encode: Schema
}

object SchemaEncoder {

  def createSchema(dataType: DataType): Schema = {
    dataType match {
      case StructType(className, simpleName, packageName, annotations, fields) =>
        val builder = SchemaBuilder.record(simpleName).namespace(packageName).doc(AnnotationExtractors.doc(annotations).orNull).fields()
        fields.foldLeft(builder) { (builder, field) =>
          val b = builder.name(field.name).doc(AnnotationExtractors.doc(field.annotations).orNull).`type`(createSchema(field.dataType))
          if (field.default == null) b.noDefault() else b.withDefault(field.default)
        }.endRecord()
      case DecimalType(precision, scale) =>
        val schema = Schema.create(Schema.Type.BYTES)
        LogicalTypes.decimal(precision, scale).addToSchema(schema)
        schema
      case NullableType(elementType) =>
        SchemaBuilder.nullable.`type`(createSchema(elementType))
      case UUIDType =>
        val schema = Schema.create(Schema.Type.STRING)
        LogicalTypes.uuid().addToSchema(schema)
        schema
      case StringType => Schema.create(Schema.Type.STRING)
      case BooleanType => Schema.create(Schema.Type.BOOLEAN)
      case DoubleType => Schema.create(Schema.Type.DOUBLE)
      case FloatType => Schema.create(Schema.Type.FLOAT)
      case LongType => Schema.create(Schema.Type.LONG)
      case IntType => Schema.create(Schema.Type.INT)
      case ShortType => Schema.create(Schema.Type.INT)
      case ByteType => Schema.create(Schema.Type.INT)
      case BinaryType => Schema.create(Schema.Type.BYTES)
      case LocalDateType =>
        val schema = Schema.create(Schema.Type.INT)
        LogicalTypes.date().addToSchema(schema)
        schema
      case LocalTimeType =>
        val schema = Schema.create(Schema.Type.INT)
        LogicalTypes.timeMillis().addToSchema(schema)
        schema
      case TimestampType =>
        val schema = Schema.create(Schema.Type.LONG)
        LogicalTypes.timestampMillis().addToSchema(schema)
        schema
      case LocalDateTimeType =>
        val schema = Schema.create(Schema.Type.LONG)
        LogicalTypes.timestampMillis().addToSchema(schema)
        schema
      case EnumType(name, symbols, annos) => SchemaBuilder.enumeration(name).symbols(symbols: _*)
      case ArrayType(elementType) => SchemaBuilder.array().items(createSchema(elementType))
      case MapType(_, valueType) => Schema.createMap(createSchema(valueType))
      case UnionType(types) => Schema.createUnion(types.map(createSchema): _*)
    }
  }

  implicit def apply[T](implicit dataTypeFor: DataTypeFor[T]): SchemaEncoder[T] = new SchemaEncoder[T] {
    override val encode: Schema = createSchema(dataTypeFor.dataType)
  }
}
