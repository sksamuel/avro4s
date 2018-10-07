package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, JsonProperties, LogicalTypes, Schema}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{Coproduct, Generic, HList}

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.runtime.universe._

/**
  * A [[Decoder]] is used to convert an Avro value, such as a [[GenericRecord]], [[org.apache.avro.specific.SpecificRecord]],
  * [[org.apache.avro.generic.GenericFixed]], or basic JVM types, into a type that is compatible with Scala.
  *
  * An example is converting between values and Scala Options - if the value is null, then the decoder
  * should emit a None. If the value is not null, then it should be wrapped in a Some.
  *
  * Another example is converting [[GenericData.Array]] or Java List into a Scala collection type.
  */
trait Decoder[T] extends Serializable {
  self =>

  /**
    * Decodes the given value to an instance of T if possible.
    * Otherwise throw an error.
    *
    * The provided schema is the reader schema.
    */
  def decode(value: Any, schema: Schema): T

  def map[U](fn: T => U): Decoder[U] = new Decoder[U] {
    override def decode(value: Any, schema: Schema): U = fn(self.decode(value, schema))
  }
}

object Decoder extends CoproductDecoders with TupleDecoders {

  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder

  /**
    * Create a decoder that always returns a single value.
    */
  final def const[A](a: A): Decoder[A] = new Decoder[A] {
    override def decode(value: Any, schema: Schema): A = a
  }

  /**
    * Create a decoder from a function.
    */
  final def instance[A](fn: (Any, Schema) => A): Decoder[A] = new Decoder[A] {
    override def decode(value: Any, schema: Schema): A = fn(value, schema)
  }

  implicit object BooleanDecoder extends Decoder[Boolean] {
    override def decode(value: Any, schema: Schema): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object ByteDecoder extends Decoder[Byte] {
    override def decode(value: Any, schema: Schema): Byte = value.asInstanceOf[Int].toByte
  }

  implicit object ShortDecoder extends Decoder[Short] {
    override def decode(value: Any, schema: Schema): Short = value match {
      case b: Byte => b
      case s: Short => s
      case i: Int => i.toShort
    }
  }

  implicit object ByteArrayDecoder extends Decoder[Array[Byte]] {
    // byte arrays can be encoded multiple ways
    override def decode(value: Any, schema: Schema): Array[Byte] = value match {
      case buffer: ByteBuffer => buffer.array
      case array: Array[Byte] => array
      case fixed: GenericData.Fixed => fixed.bytes()
    }
  }

  implicit object ByteBufferDecoder extends Decoder[ByteBuffer] {
    override def decode(value: Any, schema: Schema): ByteBuffer = ByteArrayDecoder.map(ByteBuffer.wrap).decode(value, schema)
  }

  implicit val ByteListDecoder: Decoder[List[Byte]] = ByteArrayDecoder.map(_.toList)
  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = ByteArrayDecoder.map(_.toVector)
  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = ByteArrayDecoder.map(_.toSeq)

  implicit object DoubleDecoder extends Decoder[Double] {
    override def decode(value: Any, schema: Schema): Double = value match {
      case d: Double => d
      case d: java.lang.Double => d
    }
  }

  implicit object FloatDecoder extends Decoder[Float] {
    override def decode(value: Any, schema: Schema): Float = value match {
      case f: Float => f
      case f: java.lang.Float => f
    }
  }

  implicit object IntDecoder extends Decoder[Int] {
    override def decode(value: Any, schema: Schema): Int = value match {
      case byte: Byte => byte.toInt
      case short: Short => short.toInt
      case int: Int => int
      case other => sys.error(s"Cannot convert $other to type INT")
    }
  }

  implicit object LongDecoder extends Decoder[Long] {
    override def decode(value: Any, schema: Schema): Long = value match {
      case byte: Byte => byte.toLong
      case short: Short => short.toLong
      case int: Int => int.toLong
      case long: Long => long
      case other => sys.error(s"Cannot convert $other to type LONG")
    }
  }

  implicit object LocalTimeDecoder extends Decoder[LocalTime] {
    // avro4s stores times as either millis since midnight or micros since midnight
    override def decode(value: Any, schema: Schema): LocalTime = {
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
    override def decode(value: Any, schema: Schema): String =
      value match {
        case u: Utf8 => u.toString
        case s: String => s
        case charseq: CharSequence => charseq.toString
        case bytebuf: ByteBuffer => new String(bytebuf.array)
        case a: Array[Byte] => new String(a)
        case other => sys.error(s"Cannot decode $other into a string")
      }
  }

  implicit object UUIDDecoder extends Decoder[UUID] {
    override def decode(value: Any, schema: Schema): UUID = {
      value match {
        case s: String => UUID.fromString(s)
        case bytebuf: ByteBuffer => UUID.fromString(new String(bytebuf.array))
        case a: Array[Byte] => UUID.fromString(new String(a))
        case other => sys.error(s"Cannot decode $other into a UUID")
      }
    }
  }

  implicit def optionDecoder[T](implicit decoder: Decoder[T]) = new Decoder[Option[T]] {
    override def decode(value: Any, schema: Schema): Option[T] = if (value == null) None else Option(decoder.decode(value, schema))
  }

  implicit def vectorDecoder[T](implicit decoder: Decoder[T]): Decoder[Vector[T]] = new Decoder[Vector[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Vector[T] = value match {
      case array: Array[_] => array.map(decoder.decode(_, schema)).toVector
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toVector
      case other => sys.error("Unsupported vector " + other)
    }
  }

  implicit def arrayDecoder[T](implicit decoder: Decoder[T],
                               tag: ClassTag[T]): Decoder[Array[T]] = new Decoder[Array[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Array[T] = value match {
      case array: Array[_] => array.map(decoder.decode(_, schema))
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toArray
      case other => sys.error(s"Unsupported array type ${other.getClass} " + other)
    }
  }

  implicit def setDecoder[T](implicit decoder: Decoder[T]): Decoder[Set[T]] = seqDecoder[T](decoder).map(_.toSet)

  implicit def listDecoder[T](implicit decoder: Decoder[T]): Decoder[List[T]] = new Decoder[List[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): List[T] = value match {
      case array: Array[_] => array.map(decoder.decode(_, schema)).toList
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toList
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def seqDecoder[T](implicit decoder: Decoder[T]): Decoder[Seq[T]] = new Decoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Seq[T] = value match {
      case array: Array[_] => array.map(decoder.decode(_, schema))
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toSeq
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def mapDecoder[T](implicit valueDecoder: Decoder[T]): Decoder[Map[String, T]] = new Decoder[Map[String, T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Map[String, T] = value match {
      case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> valueDecoder.decode(v, schema) }
      case other => sys.error("Unsupported map " + other)
    }
  }

  implicit def bigDecimalDecoder(implicit sp: ScalePrecisionRoundingMode = ScalePrecisionRoundingMode.default): Decoder[BigDecimal] = {
    new Decoder[BigDecimal] {
      override def decode(value: Any, schema: Schema): BigDecimal = {
        val decimalConversion = new Conversions.DecimalConversion
        val decimalType = LogicalTypes.decimal(sp.precision, sp.scale)
        val bytes = value.asInstanceOf[ByteBuffer]
        decimalConversion.fromBytes(bytes, null, decimalType)
      }
    }
  }

  implicit def eitherDecoder[A: WeakTypeTag : Decoder, B: WeakTypeTag : Decoder]: Decoder[Either[A, B]] = new Decoder[Either[A, B]] {
    override def decode(value: Any, schema: Schema): Either[A, B] = {
      val aName = weakTypeOf[A].typeSymbol.fullName
      val bName = weakTypeOf[B].typeSymbol.fullName
      safeFrom[A](value, schema).map(Left[A, B])
        .orElse(safeFrom[B](value, schema).map(Right[A, B]))
        .getOrElse(sys.error(s"Could not decode $value into Either[$aName, $bName]"))
    }
  }

  implicit def javaEnumDecoder[E <: Enum[E]](implicit tag: ClassTag[E]) = new Decoder[E] {
    override def decode(t: Any, schema: Schema): E = {
      Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], t.toString)
    }
  }

  implicit def scalaEnumDecoder[E <: Enumeration#Value](implicit tag: WeakTypeTag[E]) = new Decoder[E] {

    import scala.reflect.NameTransformer._

    val typeRef = tag.tpe match {
      case t@TypeRef(_, _, _) => t
    }

    val klass = Class.forName(typeRef.pre.typeSymbol.asClass.fullName + "$")
    val enum = klass.getField(MODULE_INSTANCE_NAME).get(null).asInstanceOf[Enumeration]

    override def decode(t: Any, schema: Schema): E = enum.withName(t.toString).asInstanceOf[E]
  }

  implicit def genCoproductSingletons[T, C <: Coproduct, L <: HList](implicit gen: Generic.Aux[T, C],
                                                                     objs: Reify.Aux[C, L],
                                                                     toList: ToList[L, T]): Decoder[T] = new Decoder[T] {
    override def decode(value: Any, schema: Schema): T = {
      val name = value.toString
      toList(objs()).find(_.toString == name).getOrElse(sys.error(s"Uknown type $name"))
    }
  }

  implicit def applyMacro[T <: Product]: Decoder[T] = macro applyMacroImpl[T]

  def applyMacroImpl[T <: Product : c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[Decoder[T]] = {

    import c.universe
    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe
    val fullName = tpe.typeSymbol.fullName
    val packageName = reflect.packageName(tpe)

    if (!reflect.isCaseClass(tpe)) {
      c.abort(c.enclosingPosition.pos, s"This macro can only encode case classes, not instance of $tpe")
    } else if (reflect.isSealed(tpe)) {
      c.abort(c.prefix.tree.pos, s"$fullName is sealed: Sealed traits/classes should be handled by coproduct generic")
    } else if (packageName.startsWith("scala")) {
      c.abort(c.prefix.tree.pos, s"$fullName is a scala type: Built in types should be handled by explicit typeclasses of SchemaFor and not this macro")
    } else {

      val valueType = reflect.isValueClass(tpe)

      // if we have a value type then we want to return an decoder that decodes
      // the backing field and wraps it in an instance of the value type
      if (valueType) {

        val valueCstr = tpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
        val backingFieldTpe = valueCstr.typeSignature

        c.Expr[Decoder[T]](
          q"""
          new _root_.com.sksamuel.avro4s.Decoder[$tpe] {
            override def decode(value: Any, schema: _root_.org.apache.avro.Schema): $tpe = {
              val decoded = _root_.com.sksamuel.avro4s.Decoder.decodeT[$backingFieldTpe](value)
              new $tpe(decoded)
            }
          }
       """
        )

      } else {

        // the companion object where the apply construction method is located
        // without this we cannot instantiate the case class
        // we also need this to extract default values
        val companion = tpe.typeSymbol.companion

        if (companion == NoSymbol) {
          val error = s"Cannot find companion object for $fullName; If you have defined a local case class, move the definition to a top level scope."
          Console.err.println(error)
          c.error(c.enclosingPosition.pos, error)
          sys.error(error)
        }

        val fields = reflect.constructorParameters(tpe).zipWithIndex.map { case ((fieldSym, fieldTpe), index) =>

          // this is the simple name of the field
          val name = fieldSym.name.decodedName.toString

          // the name we use to pull the value out of the record should take into
          // account any @AvroName annotations. If @AvroName is not defined then we
          // use the name of the field in the case class
          val resolvedFieldName = new AnnotationExtractors(reflect.annotations(fieldSym)).name.getOrElse(name)

          if (reflect.isValueClass(fieldTpe)) {

            val valueCstr = fieldTpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
            val backingFieldTpe = valueCstr.typeSignature

            // when we have a value type, the value type itself will never have existed in the data, so the
            // value in the generic record needs to be decoded by a decoder for the underlying type.
            // once we have that, we can just wrap it in the value type
            q"""{
                  val raw = record.get($name)
                  val decoded = _root_.com.sksamuel.avro4s.Decoder.decodeT[$backingFieldTpe](raw) : $backingFieldTpe
                  new $fieldTpe(decoded) : $fieldTpe
                }
             """

          } else {

            val defswithsymbols = universe.asInstanceOf[Definitions with SymbolTable with StdNames]
            val defaultGetterName = defswithsymbols.nme.defaultGetterName(defswithsymbols.nme.CONSTRUCTOR, index + 1)
            val defaultGetterMethod = tpe.companion.member(TermName(defaultGetterName.toString))

            val transient = reflect.isTransientOnField(tpe, fieldSym)

            // if the default is defined, we will use that to populate, otherwise if the field is transient
            // we will populate with none or null, otherwise an error will be raised
            if (defaultGetterMethod.isMethod) {
              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefault[$fieldTpe]($resolvedFieldName, record, schema, $companion.$defaultGetterMethod: $fieldTpe, $transient)"""
            } else {
              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefault[$fieldTpe]($resolvedFieldName, record, schema, null, $transient)"""
            }
          }
        }

        c.Expr[Decoder[T]](
          q"""
          new _root_.com.sksamuel.avro4s.Decoder[$tpe] {
            override def decode(value: Any, schema: _root_.org.apache.avro.Schema): $tpe = {
              val fullName = $fullName
              value match {
                case record: _root_.org.apache.avro.generic.GenericRecord => $companion.apply(..$fields)
                case _ => sys.error("This decoder decodes GenericRecord => " + fullName + " but has been invoked with " + value)
              }
            }
          }
          """
        )
      }
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
  //noinspection TypeCheckCanBeMatch
  def decodeFieldOrApplyDefault[T](fieldName: String,
                                   record: GenericRecord,
                                   readerSchema: Schema,
                                   scalaDefault: Any,
                                   transient: Boolean)
                                  (implicit decoder: Decoder[T]): T = {

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

  def decodeT[T](value: Any)(implicit decoder: Decoder[T]): T = decoder.decode(value, null)
}
