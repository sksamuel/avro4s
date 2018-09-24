package com.sksamuel.avro4s.internal

import java.io.Serializable

import com.sksamuel.avro4s.{DefaultNamingStrategy, NamingStrategy}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.util.{Failure, Success, Try}

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

  def moveDefaultToHead(schema: Schema, default: Any): Schema = {
    require(schema.getType == Schema.Type.UNION)
    val defaultType = default match {
      case _: String => Schema.Type.STRING
      case _: Long => Schema.Type.LONG
      case _: Int => Schema.Type.INT
      case _: Boolean => Schema.Type.BOOLEAN
      case _: Float => Schema.Type.FLOAT
      case _: Double => Schema.Type.DOUBLE
      case other => other
    }
    val (first, rest) = schema.getTypes.asScala.partition(_.getType == defaultType)
    val result = Schema.createUnion((first.headOption.toSeq ++ rest).asJava)
    schema.getObjectProps.asScala.foreach { case (k, v) => result.addProp(k, v) }
    result
  }

  def createSchema(dataType: DataType): Schema = {
    dataType match {

      // if we have a value type then we have only a single field, and we ignore the outer struct type
      case StructType(_, simpleName, packageName, annotations, fields, true) =>

        val field = fields.head

        // if we have a fixed value type then we create the special FIXED type
        val extractor = new AnnotationExtractors(annotations)

        val doc = extractor.doc.orNull
        val namespace = extractor.namespace.getOrElse(packageName)

        extractor.fixed.fold(createSchema(field.dataType)) { size =>
          SchemaBuilder.fixed(simpleName).doc(doc).namespace(namespace).size(size)
        }

      case StructType(_, simpleName, packageName, annotations, fields, _) =>

        val extractor = new AnnotationExtractors(annotations)
        val namespace = extractor.namespace.getOrElse(packageName)
        val doc = extractor.doc.orNull
        val aliases = extractor.aliases
        val props = extractor.props

        val builder = props.foldLeft(SchemaBuilder.record(simpleName).namespace(namespace).aliases(aliases: _*).doc(doc)) { case (b, (key, value)) =>
          b.prop(key, value)
        }

        fields.foldLeft(builder.fields()) { (builder, field) =>

          val extractor = new AnnotationExtractors(field.annotations)
          val doc = extractor.doc.orNull
          val aliases = extractor.aliases
          val props = extractor.props

          // the name could have been overriden with @AvroName, and then must be encoded with the naming strategy
          val name = extractor.name.fold(namingStrategy.to(field.name))(namingStrategy.to)

          // if we have annotated with @AvroFixed then we override the type and change it to a Fixed schema
          // if someone puts @AvroFixed on a complex type, it makes no sense, but that's their cross to bear
          val schema = extractor.fixed.fold(createSchema(field.dataType)) { size =>
            SchemaBuilder.fixed(name).doc(doc).namespace(namespace).size(size)
          }

          // for a union the type that has a default must be first
          val schemaWithOrderedUnion = if (schema.getType == Schema.Type.UNION && field.default != null) {
            moveDefaultToHead(schema, resolveDefault(field.default, field.dataType))
          } else schema

          // the field can override the namespace if the Namespace annotation is present on the field
          // we may have annotated our field with @AvroNamespace so this namespace should be applied
          // to any schemas we have generated for this field
          val schemaWithResolvedNamespace = extractor.namespace.map(overrideNamespace(schemaWithOrderedUnion, _)).getOrElse(schemaWithOrderedUnion)

          val b = builder.name(name).doc(doc).aliases(aliases: _*)
          val c = props.foldLeft(b) { case (bb, (key, value)) => bb.prop(key, value) }
          val d = c.`type`(schemaWithResolvedNamespace)

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
        val flattened = extractUnionSchemas(createSchema(elementType))
        val schemas = Schema.create(Schema.Type.NULL) +: flattened
        Schema.createUnion(moveNullToHead(schemas).asJava)

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

      // binary type can be handled in two ways, either bytes or fixed
      // for fixed, we look for the annotation @AvroFixe
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

      case UnionType(types) =>
        val schemas = types.map(createSchema).flatMap(extractUnionSchemas)
        Schema.createUnion(moveNullToHead(schemas).asJava)
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
  // for example, we might define a case class with a UUID field with a default value
  // of UUID.randomUUID(), but in avro UUIDs are logical types. Therefore the default
  // values must be converted into a base type avro understands.
  // another example would be `name: Option[String] = Some("abc")`, we can't use
  // the Some as the default, the inner value needs to be extracted
  def resolveDefault(default: Any, dataType: DataType): Any = {
    require(default != null)
    println(s"Resolving default = $default for $dataType")
    dataType match {
      case UUIDType => default.toString
      case StringType => default.toString
      case BooleanType => default
      case LongType => default
      case IntType => default
      case FloatType => default
      case DoubleType => default
      case ShortType => default
      case ByteType => default
      case _: DecimalType => default match {
        case bd: BigDecimal => bd.underlying()
        case bd: java.math.BigDecimal => bd
        case d: Double => d
        case f: Float => f
        case other => other.toString
      }
      case NullableType(elementType) => default match {
        case Some(value) => resolveDefault(value, elementType)
        case None => null
      }
      case _ => default.toString
    }
  }
}


