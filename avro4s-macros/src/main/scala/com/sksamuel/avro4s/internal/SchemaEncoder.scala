package com.sksamuel.avro4s.internal

import java.io.Serializable

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

trait SchemaEncoder[A] extends Serializable {
  def encode: Schema
}

object SchemaEncoder {

  import scala.collection.JavaConverters._

  private def overrideNamespace(schema: Schema, namespace: String): Schema =
    schema.getType match {
      case Schema.Type.RECORD =>
        val fields = schema.getFields.asScala.map(field =>
          new Schema.Field(field.name(), overrideNamespace(field.schema(), namespace), field.doc, field.defaultVal, field.order))
        Schema.createRecord(schema.getName, schema.getDoc, namespace, schema.isError, fields.asJava)
      case Schema.Type.UNION => Schema.createUnion(schema.getTypes.asScala.map(overrideNamespace(_, namespace)).asJava)
      case Schema.Type.ENUM => Schema.createEnum(schema.getName, schema.getDoc, namespace, schema.getEnumSymbols)
      case Schema.Type.FIXED => Schema.createFixed(schema.getName, schema.getDoc, namespace, schema.getFixedSize)
      case Schema.Type.MAP => Schema.createMap(overrideNamespace(schema.getValueType, namespace))
      case Schema.Type.ARRAY => Schema.createArray(overrideNamespace(schema.getElementType, namespace))
      case _ => schema
    }

  def createSchema(dataType: DataType): Schema = {
    dataType match {

      case StructType(_, simpleName, packageName, annotations, fields) =>

        val extractor = new AnnotationExtractors(annotations)
        val namespace = extractor.namespace.getOrElse(packageName)
        val doc = extractor.doc.orNull

        val builder = SchemaBuilder.record(simpleName).namespace(namespace).doc(doc).fields()
        fields.foldLeft(builder) { (builder, field) =>

          val extractor = new AnnotationExtractors(field.annotations)
          val doc = extractor.doc.orNull

          // this is the schema for our field
          val schema = createSchema(field.dataType)

          // the field can override the namespace if the Namespace annotation is present on the field
          // we may have annotated our field with @AvroNamespace so this namespace should be applied
          // to any schemas we have generated for this field
          val schemaWithResolvedNamespace = extractor.namespace.map(overrideNamespace(schema, _)).getOrElse(schema)

          val b = builder.name(field.name).doc(doc).`type`(schemaWithResolvedNamespace)
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

      case EnumType(_, simpleName, packageName, symbols, annos) =>
        val extractor = new AnnotationExtractors(annos)
        val namespace = extractor.namespace.getOrElse(packageName)
        SchemaBuilder.enumeration(simpleName).namespace(namespace).symbols(symbols: _*)

      case ArrayType(elementType) => SchemaBuilder.array().items(createSchema(elementType))
      case MapType(_, valueType) => Schema.createMap(createSchema(valueType))
      case UnionType(types) => Schema.createUnion(types.map(createSchema): _*)
    }
  }

  implicit def apply[T](implicit dataTypeFor: DataTypeFor[T]): SchemaEncoder[T] = new SchemaEncoder[T] {
    override val encode: Schema = createSchema(dataTypeFor.dataType)
  }
}
