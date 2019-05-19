package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import magnolia.{CaseClass, Magnolia, SealedTrait}
import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis}
import org.apache.avro.generic.{GenericContainer, GenericData, GenericFixed, GenericRecord, IndexedRecord}
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, JsonProperties, Schema}
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
  * by emitting a None, and a non-null input by wrapping the decoded value
  * in a Some.
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
    */
  def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): T

  def map[U](fn: T => U): Decoder[U] = new Decoder[U] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): U = fn(self.decode(value, schema))
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
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): A = a
  }

  /**
    * Create a decoder from a function.
    */
  final def instance[A](fn: (Any, Schema) => A): Decoder[A] = new Decoder[A] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): A = fn(value, schema)
  }

  implicit object BooleanDecoder extends Decoder[Boolean] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object ByteDecoder extends Decoder[Byte] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Byte = value.asInstanceOf[Int].toByte
  }

  implicit object ShortDecoder extends Decoder[Short] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Short = value match {
      case b: Byte => b
      case s: Short => s
      case i: Int => i.toShort
    }
  }

  implicit object ByteArrayDecoder extends Decoder[Array[Byte]] {
    // byte arrays can be encoded multiple ways
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Array[Byte] = value match {
      case buffer: ByteBuffer => buffer.array
      case array: Array[Byte] => array
      case fixed: GenericData.Fixed => fixed.bytes()
    }
  }

  implicit object ByteBufferDecoder extends Decoder[ByteBuffer] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): ByteBuffer = ByteArrayDecoder.map(ByteBuffer.wrap).decode(value, schema)
  }

  implicit val ByteListDecoder: Decoder[List[Byte]] = ByteArrayDecoder.map(_.toList)
  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = ByteArrayDecoder.map(_.toVector)
  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = ByteArrayDecoder.map(_.toSeq)

  implicit object DoubleDecoder extends Decoder[Double] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Double = value match {
      case d: Double => d
      case d: java.lang.Double => d
    }
  }

  implicit object FloatDecoder extends Decoder[Float] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Float = value match {
      case f: Float => f
      case f: java.lang.Float => f
    }
  }

  implicit object IntDecoder extends Decoder[Int] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Int = value match {
      case byte: Byte => byte.toInt
      case short: Short => short.toInt
      case int: Int => int
      case other => sys.error(s"Cannot convert $other to type INT")
    }
  }

  implicit object LongDecoder extends Decoder[Long] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Long = value match {
      case byte: Byte => byte.toLong
      case short: Short => short.toLong
      case int: Int => int.toLong
      case long: Long => long
      case other => sys.error(s"Cannot convert $other to type LONG")
    }
  }

  implicit object LocalTimeDecoder extends Decoder[LocalTime] {
    // avro4s stores times as either millis since midnight or micros since midnight
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): LocalTime = {
      schema.getLogicalType match {
        case _: TimeMicros =>
          value match {
            case i: Int => LocalTime.ofNanoOfDay(i.toLong * 1000L)
            case l: Long => LocalTime.ofNanoOfDay(l * 1000L)
          }
        case _: TimeMillis =>
          value match {
            case i: Int => LocalTime.ofNanoOfDay(i.toLong * 1000000L)
            case l: Long => LocalTime.ofNanoOfDay(l * 1000000L)
          }
      }
    }
  }

  implicit val LocalDateTimeDecoder: Decoder[LocalDateTime] = LongDecoder.map(millis => LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC))
  implicit val LocalDateDecoder: Decoder[LocalDate] = LongDecoder.map(LocalDate.ofEpochDay)
  implicit val InstantDecoder: Decoder[Instant] = LongDecoder.map(Instant.ofEpochMilli)
  implicit val DateDecoder: Decoder[Date] = LocalDateDecoder.map(Date.valueOf)
  implicit val TimestampDecoder: Decoder[Timestamp] = LongDecoder.map(new Timestamp(_))

  implicit object StringDecoder extends Decoder[String] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): String =
      value match {
        case u: Utf8 => u.toString
        case s: String => s
        case charseq: CharSequence => charseq.toString
        case bytebuf: ByteBuffer => new String(bytebuf.array)
        case a: Array[Byte] => new String(a)
        case fixed: GenericData.Fixed => new String(fixed.bytes())
        case null => sys.error("Cannot decode <null> as a string")
        case other => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
      }
  }

  implicit object UUIDDecoder extends Decoder[UUID] {
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): UUID = {
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
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Option[T] = if (value == null) None else {
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
      Option(decoder.decode(value, nonNullSchema))
    }
  }

  implicit def vectorDecoder[T](implicit decoder: Decoder[T]): Decoder[Vector[T]] = seqDecoder[T](decoder).map(_.toVector)

  implicit def arrayDecoder[T](implicit decoder: Decoder[T], classTag: ClassTag[T]): Decoder[Array[T]] = seqDecoder[T](decoder).map(_.toArray)

  implicit def setDecoder[T](implicit decoder: Decoder[T]): Decoder[Set[T]] = seqDecoder[T](decoder).map(_.toSet)

  implicit def listDecoder[T](implicit decoder: Decoder[T]): Decoder[List[T]] = seqDecoder[T](decoder).map(_.toList)

  implicit def seqDecoder[T](implicit decoder: Decoder[T]): Decoder[Seq[T]] = new Decoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Seq[T] = value match {
      case array: Array[_] => array.toSeq.map(decoder.decode(_, schema.getElementType))
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema.getElementType)).toSeq
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def mapDecoder[T](implicit valueDecoder: Decoder[T]): Decoder[Map[String, T]] = new Decoder[Map[String, T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Map[String, T] = value match {
      case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> valueDecoder.decode(v, schema.getValueType) }
      case other => sys.error("Unsupported map " + other)
    }
  }

  implicit object BigDecimalDecoder extends Decoder[BigDecimal] {
    private val converter = new Conversions.DecimalConversion
    private val fromString = StringDecoder.map(BigDecimal(_))
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): BigDecimal = schema.getType match {
      case Schema.Type.STRING => fromString.decode(value, schema)
      case Schema.Type.BYTES => ByteBufferDecoder.map(converter.fromBytes(_, schema, schema.getLogicalType)).decode(value, schema)
      case Schema.Type.FIXED => converter.fromFixed(value.asInstanceOf[GenericFixed], schema, schema.getLogicalType)
      case other => sys.error(s"Unsupported type for BigDecimals: $other, $schema")
    }
  }

  implicit def eitherDecoder[A: WeakTypeTag : Decoder : Manifest, B: WeakTypeTag : Decoder : Manifest]: Decoder[Either[A, B]] = new Decoder[Either[A, B]] {

    private[this] val safeFromA: SafeFrom[A] = SafeFrom.makeSafeFrom[A]
    private[this] val safeFromB: SafeFrom[B] = SafeFrom.makeSafeFrom[B]

//    private val nameA = Namer(implicitly[WeakTypeTag[A]].tpe).fullName
//    private val nameB = Namer(implicitly[WeakTypeTag[B]].tpe).fullName

    private val nameA = Namer(implicitly[Manifest[A]].runtimeClass).fullName
    private val nameB = Namer(implicitly[Manifest[B]].runtimeClass).fullName

    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): Either[A, B] = {

      // eithers must be a union
      require(schema.getType == Schema.Type.UNION)

      // do we have a type of A or a type of B?
      // we need to extract the schema from the union
      safeFromA.safeFrom(value, schema).map(Left[A, B])
        .orElse(safeFromB.safeFrom(value, schema).map(Right[A, B]))
        .getOrElse {
          sys.error(s"Could not decode $value into Either[$nameB, $nameB]")
        }
    }
  }

  implicit def javaEnumDecoder[E <: Enum[E]](implicit tag: ClassTag[E]) = new Decoder[E] {
    override def decode(t: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): E = {
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

    override def decode(t: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): E = {
      enum.withName(t.toString).asInstanceOf[E]
    }
  }

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
  def decodeFieldOrApplyDefault[T](fieldName: String,
                                   record: GenericRecord,
                                   readerSchema: Schema,
                                   scalaDefault: Any,
                                   transient: Boolean,
                                   decoder: Decoder[T]): T = {

    implicit class RichSchema(s: Schema) {
      def hasField(fieldName: String): Boolean = s.getField(fieldName) != null
      def hasDefault(fieldName: String): Boolean = {
        val field = s.getField(fieldName)
        field != null && field.defaultVal() != null
      }
    }

    record.getSchema.getField(fieldName) match {
      case field: Schema.Field =>
        val value = record.get(fieldName)
        decoder.decode(value, field.schema)
      case null if readerSchema.hasDefault(fieldName) =>
        val default = readerSchema.getField(fieldName).defaultVal() match {
          case JsonProperties.NULL_VALUE => null
          case other => other
        }
        decoder.decode(default, readerSchema.getField(fieldName).schema)
      case null if scalaDefault != null => scalaDefault.asInstanceOf[T]
      case null if transient => None.asInstanceOf[T]
      case _ => sys.error(s"Record $record does not have a value for $fieldName, no default was defined, and the field is not transient")
    }
  }

  def decodeT[T](value: Any, schema: Schema)(implicit decoder: Decoder[T]): T = decoder.decode(value, schema)


  def combine[T](klass: CaseClass[Typeclass, T]): Decoder[T] = {

    // In Scala, value types are erased at compile time. So in avro4s we assume that value types do not exist
    // in the avro side, neither in the schema, nor in the input values. Therefore, when creating a decoder
    // for a value type, the input will be a value of the underlying type. In other words, given a value type
    // `Foo(s: String) extends AnyVal` - the input will be a String, not a record containing a String
    // as it would be for a non-value type.

    // So the generated decoder for a value type should simply pass through to a generated decoder for
    // the underlying type without worrying about fields etc.
    if (klass.isValueClass) {
      new Decoder[T] {
        override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): T = {
          val decoded = klass.parameters.head.typeclass.decode(value, schema)
          klass.rawConstruct(List(decoded))
        }
      }
    } else {
      new Decoder[T] {
        override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): T = {
          value match {
            case record: IndexedRecord =>
              // if we are in here then we are decoding a case class so we need a record schema
              require(schema.getType == Schema.Type.RECORD)
              val values = klass.parameters.map { p =>

                // take into account @AvroName and use the naming strategy
                val name = naming.to(new AnnotationExtractors(p.annotations).name.getOrElse(p.label))

                // does the schema contain this parameter? If not, we will be relying on defaults or options
                val field = record.getSchema.getField(name)
                if (field == null) {
                  p.default match {
                    case Some(default) => default
                    case None => p.typeclass.decode(null, schema)
                  }
              //    param.default.getOrElse(sys.error(s"Record does not have field ${param.label} and the class does not define a default"))
                } else {
                  val k = record.getSchema.getFields.indexOf(field)
                  val value = record.get(k)
                  p.typeclass.decode(value, schema.getFields.get(p.index).schema())
                }
              }

              klass.rawConstruct(values)

            case _ => sys.error(s"This decoder can only handle types of IndexedRecord or it's subtypes such as GenericRecord [was ${value.getClass}]")
          }
        }
      }
    }
  }

  def decodeField[T](fieldName: String,
                     record: GenericRecord,
                     readerSchema: Schema,
                     scalaDefault: Any,
                     transient: Boolean,
                     decoder: Decoder[T]): T = {
    record.getSchema.getField(fieldName) match {
      case field: Schema.Field =>
        val value = record.get(fieldName)
        decoder.decode(value, field.schema)
      //      case null if readerSchema.hasDefault(fieldName) =>
      //        val default = readerSchema.getField(fieldName).defaultVal() match {
      //          case JsonProperties.NULL_VALUE => null
      //          case other => other
      //        }
      //        decoder.decode(default, readerSchema.getField(fieldName).schema)
      case null if scalaDefault != null => scalaDefault.asInstanceOf[T]
      case null if transient => None.asInstanceOf[T]
      case _ => sys.error(s"Record $record does not have a value for $fieldName, no default was defined, and the field is not transient")
    }
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): Decoder[T] = new Decoder[T] {
    override def decode(container: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): T = {
      schema.getType match {
        // with a record we already have the schema for the container, we don't need to do anything
        // this is how top level ADTs are encoded
        case Schema.Type.RECORD =>
          container match {
            case container: GenericContainer =>
              val subtype = ctx.subtypes.find { subtype => Namer(subtype.typeName, subtype.annotations).fullName == container.getSchema.getFullName }
                .getOrElse(sys.error(s"Could not find subtype for ${container.getSchema.getFullName} in subtypes ${ctx.subtypes}"))
              subtype.typeclass.decode(container, schema)
            case _ => sys.error(s"Unsupported type $container in sealed trait decoder")
          }
        // we have a union for nested ADTs and must extract the appropriate schema
        case Schema.Type.UNION =>
          container match {
            case container: GenericContainer =>
              val subschema = schema.getTypes.asScala.find(_.getFullName == container.getSchema.getFullName)
                .getOrElse(sys.error(s"Could not find schema for ${container.getSchema.getFullName} in union schema $schema"))
              val subtype = ctx.subtypes.find { subtype => Namer(subtype.typeName, subtype.annotations).fullName == container.getSchema.getFullName }
                .getOrElse(sys.error(s"Could not find subtype for ${container.getSchema.getFullName} in subtypes ${ctx.subtypes}"))
              subtype.typeclass.decode(container, subschema)
            case _ => sys.error(s"Unsupported type $container in sealed trait decoder")
          }
        // case objects are encoded as enums
        // we need to take the string and create the object
        case Schema.Type.ENUM =>
          container match {
            case enum: GenericData.EnumSymbol =>
              ctx.subtypes.find { subtype => Namer(subtype).name == enum.getSchema.getFullName }
                .getOrElse(sys.error(s"Could not find subtype for enum $enum"))
                .typeclass.decode(enum, enum.getSchema)
            case str: String =>
              val subtype = ctx.subtypes.find { subtype => Namer(subtype).name == str }
                .getOrElse(sys.error(s"Could not find subtype for enum $str"))
              val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
              val module = runtimeMirror.staticModule(subtype.typeName.full)
              val companion = runtimeMirror.reflectModule(module.asModule)
              companion.instance.asInstanceOf[T]
          }
        case other => sys.error(s"Unsupported sealed trait schema type $other")
      }
    }
  }

  implicit def tuple2Decoder[A, B](implicit
                                   decoderA: Decoder[A],
                                   decoderB: Decoder[B]
                                  ) = new Decoder[(A, B)] {
    override def decode(t: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): (A, B) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema)
      )
    }
  }

  implicit def tuple3Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C]
                                           ) = new Decoder[(A, B, C)] {
    override def decode(t: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): (A, B, C) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema),
        decoderC.decode(record.get("_3"), schema)
      )
    }
  }

  implicit def tuple4Decoder[A, B, C, D, E](implicit
                                            decoderA: Decoder[A],
                                            decoderB: Decoder[B],
                                            decoderC: Decoder[C],
                                            decoderD: Decoder[D]
                                           ) = new Decoder[(A, B, C, D)] {
    override def decode(t: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): (A, B, C, D) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema),
        decoderC.decode(record.get("_3"), schema),
        decoderD.decode(record.get("_4"), schema)
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
    override def decode(t: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): (A, B, C, D, E) = {
      val record = t.asInstanceOf[GenericRecord]
      (
        decoderA.decode(record.get("_1"), schema),
        decoderB.decode(record.get("_2"), schema),
        decoderC.decode(record.get("_3"), schema),
        decoderD.decode(record.get("_4"), schema),
        decoderE.decode(record.get("_5"), schema)
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
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): CNil = sys.error("This should never happen: CNil has no inhabitants")
  }

  // We're expecting to read a value of type S :+: T from avro.  Avro
  // unions are untyped, so we have to attempt to read a value of type
  // S (the concrete type), and if that fails, attempt to read the
  // rest of the coproduct type T.

  // thus, the bulk of the logic here is shared with reading Eithers, in `safeFrom`.
  implicit def coproductDecoder[S: WeakTypeTag : Manifest : Decoder, T <: Coproduct](implicit decoder: Decoder[T]): Decoder[S :+: T] = new Decoder[S :+: T] {
    private[this] val safeFromS = SafeFrom.makeSafeFrom[S]
    override def decode(value: Any, schema: Schema)(implicit naming: NamingStrategy = DefaultNamingStrategy): S :+: T = {
      safeFromS.safeFrom(value, schema) match {
        case Some(s) => Coproduct[S :+: T](s)
        case None => Inr(decoder.decode(value, schema))
      }
    }
  }
}
