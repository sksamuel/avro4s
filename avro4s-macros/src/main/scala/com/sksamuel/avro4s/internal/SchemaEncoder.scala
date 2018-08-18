package com.sksamuel.avro4s.internal

import java.io.Serializable

import com.sksamuel.avro4s.{DefaultNamingStrategy, NamingStrategy}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

trait SchemaEncoder[A] extends Serializable {
  def encode(namingStrategy: NamingStrategy = DefaultNamingStrategy): Schema
}

object SchemaEncoder {
  implicit def apply[T](implicit dataTypeFor: DataTypeFor[T]): SchemaEncoder[T] = new SchemaEncoder[T] {
    override def encode(namingStrategy: NamingStrategy): Schema = new SchemaConverter(namingStrategy).createSchema(dataTypeFor.dataType)
  }
}

class SchemaConverter(namingStrategy: NamingStrategy) {

  import scala.collection.JavaConverters._

  def createSchema(dataType: DataType): Schema = {
    dataType match {

      case StructType(_, simpleName, packageName, annotations, fields) =>

        val extractor = new AnnotationExtractors(annotations)
        val namespace = extractor.namespace.getOrElse(packageName)
        val doc = extractor.doc.orNull
        val aliases = extractor.aliases
        val props = extractor.props

        val builder = props.foldLeft(SchemaBuilder.record(simpleName).namespace(namespace).aliases(aliases: _*).doc(doc)) { case (builder, (key, value)) =>
          builder.prop(key, value)
        }
        fields.foldLeft(builder.fields()) { (builder, field) =>

          // the field name needs to be converted with the naming strategy
          val name = namingStrategy.to(field.name)

          val extractor = new AnnotationExtractors(field.annotations)
          val doc = extractor.doc.orNull
          val aliases = extractor.aliases
          val props = extractor.props

          // this is the schema for our field
          val schema = createSchema(field.dataType)

          // the field can override the namespace if the Namespace annotation is present on the field
          // we may have annotated our field with @AvroNamespace so this namespace should be applied
          // to any schemas we have generated for this field
          val schemaWithResolvedNamespace = extractor.namespace.map(overrideNamespace(schema, _)).getOrElse(schema)

          val b = builder.name(name).doc(doc).aliases(aliases: _*)
          val c = props.foldLeft(b) { case (builder, (key, value)) => builder.prop(key, value) }
          val d = b.`type`(schemaWithResolvedNamespace)

          if (field.default == null)
            d.noDefault()
          else
            d.withDefault(resolveDefault(field.default, field.dataType))

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

  // returns a default value that is compatible with the datatype
  // for example, we might define a UUID field with a default of UUID.randomUUID(), but
  // avro is not going to understand what to do with this type
  def resolveDefault(default: Any, dataType: DataType): Any = {
    require(default != null)
    println(s"Resolving default = $default for $dataType")
    dataType match {
      case UUIDType => default.toString
      case _ => default
    }
  }
}


