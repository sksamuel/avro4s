package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime}
import java.util.UUID

import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.util.Utf8

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait BaseSchemaFors {
  implicit val IntSchemaFor: SchemaFor[Int] = SchemaFor[Int](SchemaBuilder.builder.intType)
  implicit val ByteSchemaFor: SchemaFor[Byte] = IntSchemaFor.forType
  implicit val ShortSchemaFor: SchemaFor[Short] = IntSchemaFor.forType
  implicit val LongSchemaFor: SchemaFor[Long] = SchemaFor[Long](SchemaBuilder.builder.longType)
  implicit val FloatSchemaFor: SchemaFor[Float] = SchemaFor[Float](SchemaBuilder.builder.floatType)
  implicit val DoubleSchemaFor: SchemaFor[Double] = SchemaFor[Double](SchemaBuilder.builder.doubleType)
  implicit val BooleanSchemaFor: SchemaFor[Boolean] = SchemaFor[Boolean](SchemaBuilder.builder.booleanType)
  implicit val ByteBufferSchemaFor: SchemaFor[ByteBuffer] = SchemaFor[ByteBuffer](SchemaBuilder.builder.bytesType)
  implicit val CharSequenceSchemaFor: SchemaFor[CharSequence] =
    SchemaFor[CharSequence](SchemaBuilder.builder.stringType)
  implicit val StringSchemaFor: SchemaFor[String] = SchemaFor[String](SchemaBuilder.builder.stringType)
  implicit val Utf8SchemaFor: SchemaFor[Utf8] = StringSchemaFor.forType
  implicit val UUIDSchemaFor: SchemaFor[UUID] =
    SchemaFor[UUID](LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType))

  implicit def javaEnumSchemaFor[E <: Enum[_]](implicit tag: ClassTag[E]): SchemaFor[E] = {
    val typeInfo = TypeInfo.fromClass(tag.runtimeClass)
    val nameExtractor = NameExtractor(typeInfo)
    val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)

    val maybeName = tag.runtimeClass.getAnnotations.collectFirst {
      case annotation: AvroJavaName => annotation.value()
    }

    val maybeNamespace = tag.runtimeClass.getAnnotations.collectFirst {
      case annotation: AvroJavaNamespace => annotation.value()
    }

    val name = maybeName.getOrElse(nameExtractor.name)
    val namespace = maybeNamespace.getOrElse(nameExtractor.namespace)

    val maybeEnumDefault = tag.runtimeClass.getDeclaredFields.collectFirst {
      case field if field.getDeclaredAnnotations.map(_.annotationType()).contains(classOf[AvroJavaEnumDefault]) =>
        field.getName
    }

    val schema = maybeEnumDefault
      .map { enumDefault =>
        SchemaBuilder.enumeration(name).namespace(namespace).defaultSymbol(enumDefault).symbols(symbols: _*)
      }
      .getOrElse {
        SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
      }

    val props = tag.runtimeClass.getAnnotations.collect {
      case annotation: AvroJavaProp => annotation.key() -> annotation.value()
    }

    props.foreach {
      case (key, value) =>
        schema.addProp(key, value)
    }
    SchemaFor[E](schema)
  }

  def getAnnotationValue[T](annotationClass: Class[T], annotations: Seq[Annotation]): Option[String] = {
    annotations.collectFirst {
      case a: Annotation if a.tree.tpe.typeSymbol.name.toString == annotationClass.getSimpleName =>
        a.tree.children.tail.headOption.flatMap {
          case select: Select => Some(select.name.toString)
          case _              => None
        }
    }.flatten
  }

  implicit def scalaEnumSchemaFor[E <: scala.Enumeration#Value](implicit tag: TypeTag[E]): SchemaFor[E] = {

    val typeRef = tag.tpe match {
      case t @ TypeRef(_, _, _) => t
    }

//    val valueType = typeOf[E]
//    val pre = typeRef.pre.typeSymbol.typeSignature.members.sorted
//    val syms = pre
//      .filter { sym =>
//        !sym.isMethod &&
//        !sym.isType &&
//        sym.typeSignature.baseType(valueType.typeSymbol) =:= valueType
//      }
//      .map { sym =>
//        sym.name.decodedName.toString.trim
//      }

    val annotations: Seq[Annotation] = typeRef.pre.typeSymbol.annotations

    val enumObject = tag.mirror.classLoader.loadClass(typeRef.pre.typeSymbol.fullName + "$").getField("MODULE$").get(null)
    val valuesMethod = enumObject.getClass.getMethod("values")
    val syms = valuesMethod.invoke(enumObject).asInstanceOf[Iterable[E]].map(_.toString).toList

    val maybeName = getAnnotationValue(classOf[AvroName], annotations)
    val maybeNamespace = getAnnotationValue(classOf[AvroNamespace], annotations)
    val enumDefault = getAnnotationValue(classOf[AvroEnumDefault], annotations)

    val props: Seq[(String, String)] = annotations.collect {
      case a: Annotation if a.tree.tpe.typeSymbol.name.toString == classOf[AvroProp].getSimpleName =>
        a.tree.children.tail match {
          case List(key: Literal, value: Literal) => key.value.value.toString -> value.value.value.toString
          case _ =>
            throw new RuntimeException(
              "Failed to process an AvroProp annotation. The annotation should contain a key and value literals.")
        }
    }

    val nameExtractor = NameExtractor(TypeInfo.fromType(typeRef.pre))

    val name = maybeName.getOrElse(nameExtractor.name)
    val namespace = maybeNamespace.getOrElse(nameExtractor.namespace)

    val schema = enumDefault
      .map { default =>
        SchemaBuilder.enumeration(name).namespace(namespace).defaultSymbol(default) symbols (syms: _*)
      }
      .getOrElse {
        SchemaBuilder.enumeration(name).namespace(namespace).symbols(syms: _*)
      }

    props.foreach {
      case (key, value) =>
        schema.addProp(key, value)
    }
    SchemaFor[E](schema)
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

  implicit val InstantSchemaFor: SchemaFor[Instant] =
    SchemaFor[Instant](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))
  implicit val DateSchemaFor: SchemaFor[Date] = SchemaFor(
    LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType))
  implicit val LocalDateSchemaFor: SchemaFor[LocalDate] = DateSchemaFor.forType
  implicit val LocalDateTimeSchemaFor: SchemaFor[LocalDateTime] = SchemaFor(
    TimestampNanosLogicalType.addToSchema(SchemaBuilder.builder.longType))
  implicit val OffsetDateTimeSchemaFor: SchemaFor[OffsetDateTime] = SchemaFor(
    OffsetDateTimeLogicalType.addToSchema(SchemaBuilder.builder.stringType))
  implicit val LocalTimeSchemaFor: SchemaFor[LocalTime] = SchemaFor(
    LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder.longType))
  implicit val TimestampSchemaFor: SchemaFor[Timestamp] =
    SchemaFor[Timestamp](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))

  implicit def bigDecimalSchemaFor(implicit sp: ScalePrecision = ScalePrecision.default): SchemaFor[BigDecimal] =
    SchemaFor(LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType))
}

trait BaseEncoders {
  implicit object ByteEncoder extends Encoder[Byte] {
    val schemaFor: SchemaFor[Byte] = SchemaFor.ByteSchemaFor
    def encode(t: Byte): java.lang.Byte = java.lang.Byte.valueOf(t)
  }

  implicit object ShortEncoder extends Encoder[Short] {
    val schemaFor: SchemaFor[Short] = SchemaFor.ShortSchemaFor
    def encode(t: Short): java.lang.Short = java.lang.Short.valueOf(t)
  }

  implicit object IntEncoder extends Encoder[Int] {
    val schemaFor: SchemaFor[Int] = SchemaFor.IntSchemaFor
    def encode(value: Int): AnyRef = java.lang.Integer.valueOf(value)
  }

  implicit object LongEncoder extends Encoder[Long] {
    val schemaFor: SchemaFor[Long] = SchemaFor.LongSchemaFor
    def encode(value: Long): AnyRef = java.lang.Long.valueOf(value)
  }

  implicit object DoubleEncoder extends Encoder[Double] {
    val schemaFor: SchemaFor[Double] = SchemaFor.DoubleSchemaFor
    def encode(value: Double): AnyRef = java.lang.Double.valueOf(value)
  }

  implicit object FloatEncoder extends Encoder[Float] {
    val schemaFor: SchemaFor[Float] = SchemaFor.FloatSchemaFor
    def encode(value: Float): AnyRef = java.lang.Float.valueOf(value)
  }

  implicit object BooleanEncoder extends Encoder[Boolean] {
    val schemaFor: SchemaFor[Boolean] = SchemaFor.BooleanSchemaFor
    def encode(value: Boolean): AnyRef = java.lang.Boolean.valueOf(value)
  }

  implicit object ByteBufferEncoder extends Encoder[ByteBuffer] {
    val schemaFor: SchemaFor[ByteBuffer] = SchemaFor.ByteBufferSchemaFor
    def encode(value: ByteBuffer): AnyRef = value
  }

  implicit object CharSequenceEncoder extends Encoder[CharSequence] {
    val schemaFor: SchemaFor[CharSequence] = SchemaFor.CharSequenceSchemaFor
    def encode(value: CharSequence): AnyRef = value
  }

  implicit val StringEncoder: Encoder[String] = new StringEncoder(SchemaFor.StringSchemaFor)

  implicit val Utf8Codec: Encoder[Utf8] = new Encoder[Utf8] {
    val schemaFor: SchemaFor[Utf8] = SchemaFor.Utf8SchemaFor
    def encode(value: Utf8): AnyRef = value
  }

  private[avro4s] class StringEncoder(val schemaFor: SchemaFor[String]) extends Encoder[String] {

    val encoder: String => AnyRef = schema.getType match {
      case Schema.Type.STRING => new Utf8(_)
      case Schema.Type.FIXED  => encodeFixed
      case Schema.Type.BYTES =>
        str =>
          ByteBuffer.wrap(str.getBytes)
      case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
    }

    def encodeFixed(value: String): AnyRef = {
      if (value.getBytes.length > schema.getFixedSize)
        throw new Avro4sEncodingException(
          s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}",
          value,
          this)
      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema)
    }

    def encode(value: String): AnyRef = encoder(value)

    override def withSchema(schemaFor: SchemaFor[String]): Encoder[String] = new StringEncoder(schemaFor)
  }

  implicit val UUIDCodec = StringEncoder.comap[UUID](_.toString).withSchema(SchemaFor.UUIDSchemaFor)

  implicit def javaEnumEncoder[E <: Enum[E]: ClassTag] = new JavaEnumEncoder[E]

  implicit def scalaEnumEncoder[E <: Enumeration#Value: TypeTag] = new ScalaEnumEncoder[E]

  class JavaEnumEncoder[E <: Enum[E]](implicit tag: ClassTag[E]) extends Encoder[E] {
    val schemaFor: SchemaFor[E] = SchemaFor.javaEnumSchemaFor[E]
    def encode(value: E): AnyRef = new EnumSymbol(schema, value.name)
  }

  class ScalaEnumEncoder[E <: Enumeration#Value](implicit tag: TypeTag[E]) extends Encoder[E] {
    val schemaFor: SchemaFor[E] = SchemaFor.scalaEnumSchemaFor[E]
    def encode(value: E): AnyRef = new EnumSymbol(schema, value.toString)
  }
}

trait BaseDecoders {
  implicit object ByteDecoder extends Decoder[Byte] {
    val schemaFor: SchemaFor[Byte] = SchemaFor.ByteSchemaFor
    def decode(value: Any): Byte = value match {
      case b: Byte => b
      case _       => value.asInstanceOf[Int].byteValue
    }
  }

  implicit object ShortDecoder extends Decoder[Short] {
    val schemaFor: SchemaFor[Short] = SchemaFor.ShortSchemaFor
    def decode(value: Any): Short = value match {
      case b: Byte  => b
      case s: Short => s
      case i: Int   => i.toShort
    }
  }

  implicit object IntDecoder extends Decoder[Int] {
    val schemaFor: SchemaFor[Int] = SchemaFor.IntSchemaFor
    def decode(value: Any): Int = value match {
      case byte: Byte   => byte.toInt
      case short: Short => short.toInt
      case int: Int     => int
      case other        => throw new Avro4sDecodingException(s"Cannot convert $other to type INT", value, this)
    }
  }

  implicit object LongDecoder extends Decoder[Long] {
    val schemaFor: SchemaFor[Long] = SchemaFor.LongSchemaFor
    def decode(value: Any): Long = value match {
      case byte: Byte   => byte.toLong
      case short: Short => short.toLong
      case int: Int     => int.toLong
      case long: Long   => long
      case other        => throw new Avro4sDecodingException(s"Cannot convert $other to type LONG", value, this)
    }
  }

  implicit object DoubleDecoder extends Decoder[Double] {
    val schemaFor: SchemaFor[Double] = SchemaFor.DoubleSchemaFor
    def decode(value: Any): Double = value match {
      case d: Double           => d
      case d: java.lang.Double => d
    }
  }

  implicit object FloatDecoder extends Decoder[Float] {
    val schemaFor: SchemaFor[Float] = SchemaFor.FloatSchemaFor
    def decode(value: Any): Float = value match {
      case f: Float           => f
      case f: java.lang.Float => f
    }
  }

  implicit object BooleanDecoder extends Decoder[Boolean] {
    val schemaFor: SchemaFor[Boolean] = SchemaFor.BooleanSchemaFor
    def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object ByteBufferDecoder extends Decoder[ByteBuffer] {
    val schemaFor: SchemaFor[ByteBuffer] = SchemaFor.ByteBufferSchemaFor
    def decode(value: Any): ByteBuffer = value match {
      case b: ByteBuffer  => b
      case a: Array[Byte] => ByteBuffer.wrap(a)
      case _              => throw new Avro4sDecodingException(s"Unable to decode value $value to ByteBuffer", value, this)
    }
  }

  implicit object CharSequenceDecoder extends Decoder[CharSequence] {
    val schemaFor: SchemaFor[CharSequence] = SchemaFor.CharSequenceSchemaFor
    def decode(value: Any): CharSequence = value match {
      case cs: CharSequence => cs
      case _                => throw new Avro4sDecodingException(s"Unable to decode value $value to CharSequence", value, this)
    }
  }

  implicit val StringDecoder: Decoder[String] = new StringDecoder(SchemaFor.StringSchemaFor)

  implicit val Utf8Decoder: Decoder[Utf8] = new Decoder[Utf8] {
    val schemaFor: SchemaFor[Utf8] = SchemaFor.Utf8SchemaFor
    def decode(value: Any): Utf8 = value match {
      case u: Utf8        => u
      case b: Array[Byte] => new Utf8(b)
      case null           => throw new Avro4sDecodingException("Cannot decode <null> as utf8", value, this)
      case _              => new Utf8(value.toString)
    }
  }

  private[avro4s] class StringDecoder(val schemaFor: SchemaFor[String]) extends Decoder[String] {

    def decode(value: Any): String = value match {
      case u: Utf8             => u.toString
      case s: String           => s
      case chars: CharSequence => chars.toString
      case fixed: GenericFixed => new String(fixed.bytes())
      case a: Array[Byte]      => new String(a)
      case null                => throw new Avro4sDecodingException("Cannot decode <null> as a string", value, this)
      case other =>
        throw new Avro4sDecodingException(s"Cannot decode $other of type ${other.getClass} into a string", value, this)
    }

    override def withSchema(schemaFor: SchemaFor[String]): Decoder[String] = new StringDecoder(schemaFor)
  }

  implicit val UUIDDecoder = StringDecoder.map[UUID](UUID.fromString).withSchema(SchemaFor.UUIDSchemaFor)

  implicit def javaEnumDecoder[E <: Enum[E]: ClassTag] = new JavaEnumDecoder[E]

  implicit def scalaEnumDecoder[E <: Enumeration#Value: TypeTag] = new ScalaEnumDecoder[E]

  class JavaEnumDecoder[E <: Enum[E]](implicit tag: ClassTag[E]) extends Decoder[E] {
    val schemaFor: SchemaFor[E] = SchemaFor.javaEnumSchemaFor[E]
    def decode(value: Any): E = Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], value.toString)
  }

  class ScalaEnumDecoder[E <: Enumeration#Value](implicit tag: TypeTag[E]) extends Decoder[E] {
    val enum = tag.tpe match {
      case TypeRef(enumType, _, _) =>
        val moduleSymbol = enumType.termSymbol.asModule
        val mirror: Mirror = runtimeMirror(getClass.getClassLoader)
        mirror.reflectModule(moduleSymbol).instance.asInstanceOf[Enumeration]
    }

    val schemaFor: SchemaFor[E] = SchemaFor.scalaEnumSchemaFor[E]

    def decode(value: Any): E = enum.withName(value.toString).asInstanceOf[E]
  }
}
