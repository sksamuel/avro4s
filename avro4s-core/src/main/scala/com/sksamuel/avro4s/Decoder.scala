package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneOffset}
import java.util.UUID

import com.sksamuel.avro4s.DecoderHelper.tryDecode
import com.sksamuel.avro4s.SchemaFor.TimestampNanosLogicalType
import magnolia.{CaseClass, Magnolia, SealedTrait}
import org.apache.avro.LogicalTypes.{Decimal, TimeMicros, TimeMillis, TimestampMicros, TimestampMillis}
import org.apache.avro.generic.{GenericContainer, GenericData, GenericEnumSymbol, GenericFixed, GenericRecord, IndexedRecord}
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, Schema}
import shapeless.{:+:, CNil, Coproduct, Inr}

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
  * A [[Decoder]] is used to convert an Avro value, such as a GenericRecord,
  * SpecificRecord, GenericFixed, EnumSymbol, or a basic JVM type, into a
  * target Scala type.
  *
  * For example, a Decoder[String] would convert an input of type Utf8 -
  * which is one of the ways Avro can encode strings - into a plain Java String.
  *
  * Another example, a decoder for Option[String] would handle inputs of null
  * by emitting a None, and a non-null input by emitting the decoded value
  * wrapped in a Some.
  *
  * A final example is converting a GenericData.Array or a Java collection type
  * into a Scala collection type.
  */
trait Decoder[T] extends Serializable {
  self =>

  /**
    * Decodes the given value to an instance of T if possible.
    * Otherwise throw an error.
    *
    * The provided schema is the reader schema.
    *
    * @param fieldMapper the [[FieldMapper]] is used when decoding container types
    *        with nested fields. Fields may have a different name in the
    *        incoming message compared to the class field names, and the
    *        namer will map between them.
    */
  def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): T

  def map[U](fn: T => U): Decoder[U] = new Decoder[U] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): U = fn(self.decode(value, schema, fieldMapper))
  }
}

object Decoder {

  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder

  type Typeclass[T] = Decoder[T]

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  /**
    * Create a decoder that always returns a single value.
    */
  final def const[A](a: A): Decoder[A] = new Decoder[A] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): A = a
  }

  /**
    * Create a decoder from a function.
    */
  final def instance[A](fn: (Any, Schema) => A): Decoder[A] = new Decoder[A] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): A = fn(value, schema)
  }

  implicit object BooleanDecoder extends Decoder[Boolean] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object ByteDecoder extends Decoder[Byte] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Byte = value.asInstanceOf[Int].toByte
  }

  implicit object ShortDecoder extends Decoder[Short] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Short = value match {
      case b: Byte => b
      case s: Short => s
      case i: Int => i.toShort
    }
  }

  implicit object ByteArrayDecoder extends Decoder[Array[Byte]] {
    // byte arrays can be encoded multiple ways
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Array[Byte] = value match {
      case buffer: ByteBuffer => buffer.array
      case array: Array[Byte] => array
      case fixed: GenericFixed => fixed.bytes()
    }
  }

  implicit object ByteBufferDecoder extends Decoder[ByteBuffer] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): ByteBuffer = ByteArrayDecoder.map(ByteBuffer.wrap).decode(value, schema, fieldMapper)
  }

  implicit val ByteListDecoder: Decoder[List[Byte]] = ByteArrayDecoder.map(_.toList)
  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = ByteArrayDecoder.map(_.toVector)
  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = ByteArrayDecoder.map(_.toSeq)

  implicit object DoubleDecoder extends Decoder[Double] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Double = value match {
      case d: Double => d
      case d: java.lang.Double => d
    }
  }

  implicit object FloatDecoder extends Decoder[Float] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Float = value match {
      case f: Float => f
      case f: java.lang.Float => f
    }
  }

  implicit object IntDecoder extends Decoder[Int] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Int = value match {
      case byte: Byte => byte.toInt
      case short: Short => short.toInt
      case int: Int => int
      case other => sys.error(s"Cannot convert $other to type INT")
    }
  }

  implicit object LongDecoder extends Decoder[Long] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Long = value match {
      case byte: Byte => byte.toLong
      case short: Short => short.toLong
      case int: Int => int.toLong
      case long: Long => long
      case other => sys.error(s"Cannot convert $other to type LONG")
    }
  }

  implicit object LocalTimeDecoder extends Decoder[LocalTime] {
    // avro4s stores times as either millis since midnight or micros since midnight
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): LocalTime = {
      schema.getLogicalType match {
        case _: TimeMillis =>
          value match {
            case i: Int => LocalTime.ofNanoOfDay(i.toLong * 1000000L)
            case l: Long => LocalTime.ofNanoOfDay(l * 1000000L)
          }
        case _: TimeMicros =>
          value match {
            case i: Int => LocalTime.ofNanoOfDay(i.toLong * 1000L)
            case l: Long => LocalTime.ofNanoOfDay(l * 1000L)
          }
      }
    }
  }

  implicit object OffsetDateTimeDecoder extends Decoder[OffsetDateTime] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper) =
      OffsetDateTime.parse(value.toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

  implicit val LocalDateTimeDecoder: Decoder[LocalDateTime] = new Decoder[LocalDateTime] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): LocalDateTime = {
      schema.getLogicalType match {
        case _: TimestampMillis =>
          value match {
            case i: Int => LocalDateTime.ofInstant(Instant.ofEpochMilli(i.toLong), ZoneOffset.UTC)
            case l: Long => LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC)
          }

        case _: TimestampMicros =>
          value match {
            case i: Int => LocalDateTime.ofInstant(Instant.ofEpochMilli(i / 1000), ZoneOffset.UTC).plusNanos(i % 1000 * 1000)
            case l: Long => LocalDateTime.ofInstant(Instant.ofEpochMilli(l / 1000), ZoneOffset.UTC).plusNanos(l % 1000 * 1000)
          }

        case TimestampNanosLogicalType =>
          value match {
            case l: Long =>
              val nanos = l % 1000000
              LocalDateTime.ofInstant(Instant.ofEpochMilli(l / 1000000), ZoneOffset.UTC).plusNanos(nanos)
            case other => sys.error(s"Unsupported type for timestamp nanos ${other.getClass.getName}")
          }
      }
    }
  }

  implicit val LocalDateDecoder: Decoder[LocalDate] = LongDecoder.map(LocalDate.ofEpochDay)
  implicit val InstantDecoder: Decoder[Instant] = LongDecoder.map(Instant.ofEpochMilli)
  implicit val DateDecoder: Decoder[Date] = LocalDateDecoder.map(Date.valueOf)
  implicit val TimestampDecoder: Decoder[Timestamp] = LongDecoder.map(new Timestamp(_))

  implicit object StringDecoder extends Decoder[String] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): String =
      value match {
        case u: Utf8 => u.toString
        case s: String => s
        case charseq: CharSequence => charseq.toString
        case bytebuf: ByteBuffer => new String(bytebuf.array)
        case a: Array[Byte] => new String(a)
        case fixed: GenericFixed => new String(fixed.bytes())
        case null =>
          sys.error("Cannot decode <null> as a string")
        case other => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
      }
  }

  implicit object Utf8Decoder extends Decoder[Utf8] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Utf8 = new Utf8(value.toString)
  }

  implicit object UUIDDecoder extends Decoder[UUID] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): UUID = {
      value match {
        case s: String => UUID.fromString(s)
        case u: Utf8 => UUID.fromString(u.toString)
        case bytebuf: ByteBuffer => UUID.fromString(new String(bytebuf.array))
        case a: Array[Byte] => UUID.fromString(new String(a))
        case null => sys.error("Cannot decode <null> as a UUID")
        case other => sys.error(s"Cannot decode $other of type ${other.getClass} into a UUID")
      }
    }
  }

  implicit def optionDecoder[T](implicit decoder: Decoder[T]) = new Decoder[Option[T]] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = if (value == null) None else {
      // Options are Union schemas of ["null", other], the decoder may require the other schema
      val schemas = if (schema.getType == Schema.Type.UNION) {
        schema.getTypes.asScala.toList
      } else {
        List(schema)
      }
      val nonNullSchema = schemas.filter(_.getType != Schema.Type.NULL) match {
        case s :: Nil => s
        case multipleSchemas => Schema.createUnion(multipleSchemas.asJava)
      }
      Option(decoder.decode(value, nonNullSchema, fieldMapper))
    }
  }

  implicit def vectorDecoder[T](implicit decoder: Decoder[T]): Decoder[Vector[T]] = seqDecoder[T](decoder).map(_.toVector)

  implicit def arrayDecoder[T](implicit decoder: Decoder[T], classTag: ClassTag[T]): Decoder[Array[T]] = seqDecoder[T](decoder).map(_.toArray)

  implicit def setDecoder[T](implicit decoder: Decoder[T]): Decoder[Set[T]] = seqDecoder[T](decoder).map(_.toSet)

  implicit def listDecoder[T](implicit decoder: Decoder[T]): Decoder[List[T]] = seqDecoder[T](decoder).map(_.toList)

  implicit def mutableSeqDecoder[T](implicit decoder: Decoder[T]): Decoder[scala.collection.mutable.Seq[T]] = seqDecoder[T](decoder).map(_.toBuffer)

  implicit def seqDecoder[T](implicit decoder: Decoder[T]): Decoder[Seq[T]] = new Decoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Seq[T] = {
      value match {
        case array: Array[_] =>
          array.toSeq.map(decoder.decode(_, schema.getElementType, fieldMapper))
        case list: java.util.Collection[_] =>
          list.asScala.map(decoder.decode(_, schema.getElementType, fieldMapper)).toSeq
        case list: List[_] =>
          list.map(decoder.decode(_, schema.getElementType, fieldMapper))
        case other => sys.error("Unsupported array " + other)
      }
    }

  }

  implicit def mapDecoder[T](implicit valueDecoder: Decoder[T]): Decoder[Map[String, T]] = new Decoder[Map[String, T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Map[String, T] = value match {
      case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> valueDecoder.decode(v, schema.getValueType, fieldMapper) }
      case other => sys.error("Unsupported map " + other)
    }
  }

  implicit object BigDecimalDecoder extends Decoder[BigDecimal] {

    private val converter = new Conversions.DecimalConversion

    private val fromString = StringDecoder.map(BigDecimal(_))
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): BigDecimal = {

      val decimal = schema.getLogicalType.asInstanceOf[Decimal]

      schema.getType match {
        case Schema.Type.STRING => fromString.decode(value, schema, fieldMapper)
        case Schema.Type.BYTES => ByteBufferDecoder.map(converter.fromBytes(_, schema, decimal)).decode(value, schema, fieldMapper)
        case Schema.Type.FIXED => converter.fromFixed(value.asInstanceOf[GenericFixed], schema, decimal)
        case Schema.Type.LONG | Schema.Type.INT => value match {
          case long: Long => BigDecimal(BigInt(long), decimal.getScale)
          case long: java.lang.Long => BigDecimal(BigInt(long), decimal.getScale)
          case int: Int => BigDecimal(BigInt(int), decimal.getScale)
          case int: java.lang.Integer => BigDecimal(BigInt(int), decimal.getScale)
        }
        case other => sys.error(s"Unsupported type for BigDecimals: $other, $schema")
      }
    }
  }

  implicit def eitherDecoder[A: WeakTypeTag : Decoder : Manifest, B: WeakTypeTag : Decoder : Manifest]: Decoder[Either[A, B]] = new Decoder[Either[A, B]] {

    private[this] val safeFromA: SafeFrom[A] = SafeFrom.makeSafeFrom[A]
    private[this] val safeFromB: SafeFrom[B] = SafeFrom.makeSafeFrom[B]

//    private val nameA = Namer(implicitly[WeakTypeTag[A]].tpe).fullName
//    private val nameB = Namer(implicitly[WeakTypeTag[B]].tpe).fullName

    private val nameA = NameExtractor(implicitly[Manifest[A]].runtimeClass).fullName
    private val nameB = NameExtractor(implicitly[Manifest[B]].runtimeClass).fullName

    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): Either[A, B] = {

      // eithers must be a union
      require(schema.getType == Schema.Type.UNION)

      // do we have a type of A or a type of B?
      // we need to extract the schema from the union
      safeFromA.safeFrom(value, schema, fieldMapper).map(Left[A, B])
        .orElse(safeFromB.safeFrom(value, schema, fieldMapper).map(Right[A, B]))
        .getOrElse {
          sys.error(s"Could not decode $value into Either[$nameB, $nameB]")
        }
    }
  }

  implicit def javaEnumDecoder[E <: Enum[E]](implicit tag: ClassTag[E]) = new Decoder[E] {
    override def decode(t: Any, schema: Schema, fieldMapper: FieldMapper): E = {
      Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], t.toString)
    }
  }

  implicit def scalaEnumDecoder[E <: Enumeration#Value](implicit tag: WeakTypeTag[E]) = new Decoder[E] {

    val mirror: Mirror = runtimeMirror(getClass.getClassLoader)

    val enum = tag.tpe match {
      case TypeRef(enumType, _, _) =>
        val moduleSymbol = enumType.termSymbol.asModule
        mirror.reflectModule(moduleSymbol).instance.asInstanceOf[Enumeration]
    }

    override def decode(t: Any, schema: Schema, fieldMapper: FieldMapper): E = {
      enum.withName(t.toString).asInstanceOf[E]
    }
  }


  def combine[T](ctx: CaseClass[Typeclass, T]): Decoder[T] = {

    // In Scala, value types are erased at compile time. So in avro4s we assume that value types do not exist
    // in the avro side, neither in the schema, nor in the input values. Therefore, when creating a decoder
    // for a value type, the input will be a value of the underlying type. In other words, given a value type
    // `Foo(s: String) extends AnyVal` - the input will be a String, not a record containing a String
    // as it would be for a non-value type.

    // So the generated decoder for a value type should simply pass through to a generated decoder for
    // the underlying type without worrying about fields etc.
    if (ctx.isValueClass) {
      new Decoder[T] {
        override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): T = {
          val decoded = ctx.parameters.head.typeclass.decode(value, schema, fieldMapper)
          ctx.rawConstruct(List(decoded))
        }
      }
    } else {
      new Decoder[T] {
        override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): T = {

          value match {
            case record: IndexedRecord =>
              // if we are in here then we are decoding a case class so we need a record schema
              require(schema.getType == Schema.Type.RECORD)
              val values = ctx.parameters.map { p =>

                /**
                  * For a field in the target type (the case class we are marshalling to), we must
                  * try to pull a value from the Avro GenericRecord. After the value has been retrieved,
                  * it needs to be decoded into the appropriate Scala type.
                  *
                  * If the writer schema does not have an entry for the field then we can consider schema
                  * evolution using the following rules in the given order.
                  *
                  * 1. If the reader schema contains a default for this field, we will use that default.
                  * 2. If the parameter is defined with a scala default method then we will use that default value.
                  * 3. If the field is marked as @transient
                  *
                  * If none of these rules can be satisfied then an exception will be thrown.
                  */

                val extractor = new AnnotationExtractors(p.annotations)

                /**
                  * We may have a schema with a field in snake case like say { "first_name": "sam" } and
                  * that schema needs to be used for a case class field `firstName`.
                  * The field mapper is used to map fields in a schema to fields in a case class by
                  * transforming the class field name to the wire name format.
                  */
                val name = extractor.name.getOrElse(fieldMapper.to(p.label))

                // take into account @AvroName and use the field mapper to get the name of this parameter in the schema
                def field = record.getSchema.getField(name)

                // does the schema contain this parameter? If not, we will be relying on defaults or options in the case class
                if (extractor.transient || field == null) {
                  p.default match {
                    case Some(default) => default
                      // there is no default, so the field must be an option
                    case None => p.typeclass.decode(null, record.getSchema, fieldMapper)
                  }
                } else {
                  val k = record.getSchema.getFields.indexOf(field)
                  val value = record.get(k)
                  tryDecode(fieldMapper, schema, p, value)
                }
              }
              ctx.rawConstruct(values)
            case enum: GenericData.EnumSymbol =>
              val res = ctx.parameters.map { p =>
                tryDecode(fieldMapper, schema, p, value)
              }
              ctx.rawConstruct(res)
            case _ => sys.error(s"This decoder can only handle types of EnumSymbol, IndexedRecord or it's subtypes such as GenericRecord [was ${value.getClass}]")
          }
        }
      }
    }
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): Decoder[T] = new Decoder[T] {
    override def decode(container: Any, schema: Schema, fieldMapper: FieldMapper): T = {
      schema.getType match {
        case Schema.Type.RECORD =>
          container match {
            case container: GenericContainer =>
              val subtype = ctx.subtypes.find { subtype => NameExtractor(subtype.typeName, subtype.annotations ++ ctx.annotations).fullName == container.getSchema.getFullName }
                .getOrElse(sys.error(s"Could not find subtype for ${container.getSchema.getFullName} in subtypes ${ctx.subtypes}"))
              subtype.typeclass.decode(container, schema, fieldMapper)
            case _ => sys.error(s"Unsupported type $container in sealed trait decoder")
          }
        // we have a union for nested ADTs and must extract the appropriate schema
        case Schema.Type.UNION =>
          container match {
            case container: GenericContainer =>
              val subschema = schema.getTypes.asScala.find(_.getFullName == container.getSchema.getFullName)
                .getOrElse(sys.error(s"Could not find schema for ${container.getSchema.getFullName} in union schema $schema"))
              val subtype = ctx.subtypes.find { subtype => NameExtractor(subtype.typeName, subtype.annotations).fullName == container.getSchema.getFullName }
                .getOrElse(sys.error(s"Could not find subtype for ${container.getSchema.getFullName} in subtypes ${ctx.subtypes}"))
              subtype.typeclass.decode(container, subschema, fieldMapper)
            case _ => sys.error(s"Unsupported type $container in sealed trait decoder")
          }
        // case objects are encoded as enums
        // we need to take the string and create the object
        case Schema.Type.ENUM =>
          val subtype = container match {
            case enum: GenericEnumSymbol =>
                ctx.subtypes.find { subtype => NameExtractor(subtype).name == enum.toString }
                  .getOrElse(sys.error(s"Could not find subtype for enum $enum"))
            case str: String =>
              ctx.subtypes.find { subtype => NameExtractor(subtype).name == str }
                .getOrElse(sys.error(s"Could not find subtype for enum $str"))
          }
          val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
          val module = runtimeMirror.staticModule(subtype.typeName.full)
          val companion = runtimeMirror.reflectModule(module.asModule)
          companion.instance.asInstanceOf[T]
        case other => sys.error(s"Unsupported sealed trait schema type $other")
      }
    }
  }

  implicit def tuple2Decoder[A, B](implicit
                                   decoderA: Decoder[A],
                                   decoderB: Decoder[B]
                                  ) = new Decoder[(A, B)] {
    override def decode(t: Any, schema: Schema, fieldMapper: FieldMapper): (A, B) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema.getField("_1").schema(), fieldMapper),
        decoderB.decode(record.get("_2"), schema.getField("_2").schema(), fieldMapper)
      )
    }
  }

  implicit def tuple3Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C]
                                           ) = new Decoder[(A, B, C)] {
    override def decode(t: Any, schema: Schema, fieldMapper: FieldMapper): (A, B, C) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema.getField("_1").schema(), fieldMapper),
        decoderB.decode(record.get("_2"), schema.getField("_2").schema(), fieldMapper),
        decoderC.decode(record.get("_3"), schema.getField("_3").schema(), fieldMapper)
      )
    }
  }

  implicit def tuple4Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C],
                                            decoderD: Decoder[D]
                                           ) = new Decoder[(A, B, C, D)] {
    override def decode(t: Any, schema: Schema, fieldMapper: FieldMapper): (A, B, C, D) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema.getField("_1").schema(), fieldMapper),
        decoderB.decode(record.get("_2"), schema.getField("_2").schema(), fieldMapper),
        decoderC.decode(record.get("_3"), schema.getField("_3").schema(), fieldMapper),
        decoderD.decode(record.get("_4"), schema.getField("_4").schema(), fieldMapper)
      )
    }
  }

  implicit def tuple5Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C],
                                            decoderD: Decoder[D],
                                            decoderE: Decoder[E]
                                           ) = new Decoder[(A, B, C, D, E)] {
    override def decode(t: Any, schema: Schema, fieldMapper: FieldMapper): (A, B, C, D, E) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema.getField("_1").schema(), fieldMapper),
        decoderB.decode(record.get("_2"), schema.getField("_2").schema(), fieldMapper),
        decoderC.decode(record.get("_3"), schema.getField("_3").schema(), fieldMapper),
        decoderD.decode(record.get("_4"), schema.getField("_4").schema(), fieldMapper),
        decoderE.decode(record.get("_5"), schema.getField("_5").schema(), fieldMapper)
      )
    }
  }

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))

  // `decode` here should never be invoked under normal operation; if
  // we're trying to read a value of type CNil it's because we've
  // tried all the other cases and failed. But the Decoder[CNil]
  // needs to exist to supply a base case for the recursion.
  implicit object CNilDecoderValue extends Decoder[CNil] {
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): CNil = sys.error("This should never happen: CNil has no inhabitants")
  }

  // We're expecting to read a value of type S :+: T from avro.  Avro
  // unions are untyped, so we have to attempt to read a value of type
  // S (the concrete type), and if that fails, attempt to read the
  // rest of the coproduct type T.

  // thus, the bulk of the logic here is shared with reading Eithers, in `safeFrom`.
  implicit def coproductDecoder[S: WeakTypeTag : Manifest : Decoder, T <: Coproduct](implicit decoder: Decoder[T]): Decoder[S :+: T] = new Decoder[S :+: T] {
    private[this] val safeFromS = SafeFrom.makeSafeFrom[S]
    override def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): S :+: T = {
      safeFromS.safeFrom(value, schema, fieldMapper) match {
        case Some(s) => Coproduct[S :+: T](s)
        case None => Inr(decoder.decode(value, schema, fieldMapper))
      }
    }
  }
}
