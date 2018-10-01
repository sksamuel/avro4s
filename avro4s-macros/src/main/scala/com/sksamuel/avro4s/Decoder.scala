package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, LogicalTypes}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{Coproduct, Generic, HList}

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.runtime.universe._

trait Decoder[T] extends Serializable {
  self =>

  def decode(value: Any): T

  def map[U](fn: T => U): Decoder[U] = new Decoder[U] {
    override def decode(value: Any): U = fn(self.decode(value))
  }
}

object Decoder extends LowPriorityDecoders {

  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder

  implicit object BooleanDecoder extends Decoder[Boolean] {
    override def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object ByteDecoder extends Decoder[Byte] {
    override def decode(value: Any): Byte = value.asInstanceOf[Int].toByte
  }

  implicit object ShortDecoder extends Decoder[Short] {
    override def decode(value: Any): Short = value.asInstanceOf[Int].toShort
  }

  // specialized support for arrays of bytes which are likely to have been serialized as a byte buffer
  // but could also by an array of bytes
  implicit object ByteArrayDecoder extends Decoder[Array[Byte]] {
    override def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer => buffer.array
      case array: Array[Byte] => array
    }
  }

  implicit object ByteBufferDecoder extends Decoder[ByteBuffer] {
    override def decode(value: Any): ByteBuffer = ByteArrayDecoder.map(ByteBuffer.wrap).decode(value)
  }

  implicit object ByteListDecoder extends Decoder[List[Byte]] {
    override def decode(value: Any): List[Byte] = ByteArrayDecoder.decode(value).toList
  }

  implicit object ByteVectorDecoder extends Decoder[Vector[Byte]] {
    override def decode(value: Any): Vector[Byte] = ByteArrayDecoder.decode(value).toVector
  }

  implicit object ByteSeqDecoder extends Decoder[Seq[Byte]] {
    override def decode(value: Any): Seq[Byte] = ByteArrayDecoder.decode(value).toSeq
  }

  implicit object DoubleDecoder extends Decoder[Double] {
    override def decode(value: Any): Double = value match {
      case d: Double => d
      case d: java.lang.Double => d
    }
  }

  implicit object FloatDecoder extends Decoder[Float] {
    override def decode(value: Any): Float = value match {
      case f: Float => f
      case f: java.lang.Float => f
    }
  }

  implicit object IntDecoder extends Decoder[Int] {
    override def decode(value: Any): Int = value match {
      case byte: Byte => byte.toInt
      case short: Short => short.toInt
      case int: Int => int
      case other => sys.error(s"Cannot convert $other to type INT")
    }
  }

  implicit object LongDecoder extends Decoder[Long] {
    override def decode(value: Any): Long = value match {
      case byte: Byte => byte.toLong
      case short: Short => short.toLong
      case int: Int => int.toLong
      case long: Long => long
      case other => sys.error(s"Cannot convert $other to type LONG")
    }
  }

  implicit object LocalTimeDecoder extends Decoder[LocalTime] {
    // avro4s stores times as either millis since midnight or micros since midnight
    override def decode(value: Any): LocalTime = value match {
      case millis: Int => LocalTime.ofNanoOfDay(millis.toLong * 1000000)
      case micros: Long => LocalTime.ofNanoOfDay(micros * 1000)
    }
  }

  implicit object LocalDateTimeDecoder extends Decoder[LocalDateTime] {
    override def decode(value: Any): LocalDateTime = LongDecoder.map(millis => LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)).decode(value)
  }

  implicit object LocalDateDecoder extends Decoder[LocalDate] {
    override def decode(value: Any): LocalDate = LongDecoder.map(LocalDate.ofEpochDay).decode(value)
  }

  implicit object InstantDecoder extends Decoder[Instant] {
    override def decode(value: Any): Instant = LongDecoder.map(Instant.ofEpochMilli).decode(value)
  }

  implicit object DateDecoder extends Decoder[Date] {
    override def decode(value: Any): Date = LocalDateDecoder.map(Date.valueOf).decode(value)
  }

  implicit object TimestampDecoder extends Decoder[Timestamp] {
    override def decode(value: Any): Timestamp = LongDecoder.map(new Timestamp(_)).decode(value)
  }

  implicit object StringDecoder extends Decoder[String] {
    override def decode(value: Any): String =
      value match {
        case null => sys.error("Cannot decode <null> into a string")
        case u: Utf8 => u.toString
        case s: String => s
        case a: Array[Byte] => new String(a)
        case other => other.toString
      }
  }

  implicit object UUIDDecoder extends Decoder[UUID] {
    override def decode(value: Any): UUID = UUID.fromString(value.toString)
  }

  implicit def optionDecoder[T](implicit decoder: Decoder[T]) = new Decoder[Option[T]] {
    override def decode(value: Any): Option[T] = if (value == null) None else Option(decoder.decode(value))
  }

  implicit def vectorDecoder[T](implicit decoder: Decoder[T]): Decoder[Vector[T]] = new Decoder[Vector[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any): Vector[T] = value match {
      case array: Array[_] => array.map(decoder.decode).toVector
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toVector
      case other => sys.error("Unsupported vector " + other)
    }
  }

  implicit def arrayDecoder[T](implicit decoder: Decoder[T],
                               tag: ClassTag[T]): Decoder[Array[T]] = new Decoder[Array[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any): Array[T] = value match {
      case array: Array[_] => array.map(decoder.decode)
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toArray
      case other => sys.error(s"Unsupported array type ${other.getClass} " + other)
    }
  }

  implicit def setDecoder[T](implicit decoder: Decoder[T]): Decoder[Set[T]] = new Decoder[Set[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any): Set[T] = value match {
      case array: Array[_] => array.map(decoder.decode).toSet
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toSet
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def listDecoder[T](implicit decoder: Decoder[T]): Decoder[List[T]] = new Decoder[List[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any): List[T] = value match {
      case array: Array[_] => array.map(decoder.decode).toList
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toList
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def seqDecoder[T](implicit decoder: Decoder[T]): Decoder[Seq[T]] = new Decoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any): Seq[T] = value match {
      case array: Array[_] => array.map(decoder.decode)
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode).toSeq
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def mapDecoder[T](implicit valueDecoder: Decoder[T]): Decoder[Map[String, T]] = new Decoder[Map[String, T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any): Map[String, T] = value match {
      case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> valueDecoder.decode(v) }
      case other => sys.error("Unsupported map " + other)
    }
  }

  implicit def bigDecimalDecoder(implicit sp: ScalePrecisionRoundingMode = ScalePrecisionRoundingMode.default): Decoder[BigDecimal] = {
    new Decoder[BigDecimal] {
      override def decode(value: Any): BigDecimal = {
        val decimalConversion = new Conversions.DecimalConversion
        val decimalType = LogicalTypes.decimal(sp.precision, sp.scale)
        val bytes = value.asInstanceOf[ByteBuffer]
        decimalConversion.fromBytes(bytes, null, decimalType)
      }
    }
  }

  implicit def eitherDecoder[A: WeakTypeTag : Decoder, B: WeakTypeTag : Decoder]: Decoder[Either[A, B]] = new Decoder[Either[A, B]] {
    override def decode(value: Any): Either[A, B] = {
      safeFrom[A](value).map(Left[A, B])
        .orElse(safeFrom[B](value).map(Right[A, B]))
        .getOrElse(sys.error(s"Could not decode $value into an either"))
    }
  }

  implicit def javaEnumDecoder[E <: Enum[E]](implicit tag: ClassTag[E]) = new Decoder[E] {
    override def decode(t: Any): E = {
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

    override def decode(t: Any): E = enum.withName(t.toString).asInstanceOf[E]
  }

  implicit def genCoproductSingletons[T, C <: Coproduct, L <: HList](implicit gen: Generic.Aux[T, C],
                                                                     objs: Reify.Aux[C, L],
                                                                     toList: ToList[L, T]): Decoder[T] = new Decoder[T] {
    override def decode(value: Any): T = {
      val name = value.toString
      toList(objs()).find(_.toString == name).getOrElse(sys.error(s"Uknown type $name"))
    }
  }

  implicit def applyMacro[T]: Decoder[T] = macro applyMacroImpl[T]

  def applyMacroImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[Decoder[T]] = {

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
            override def decode(value: Any): $tpe = {
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

        val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((fieldSym, fieldTpe), index) =>

          // this is the simple name of the field
          val name = fieldSym.name.decodedName.toString

          // the name we use to pull the value out of the record should take into
          // account any @AvroName annotations. If @AvroName is not defined then we
          // use the name of the field in the case class
          val resolvedFieldName = new AnnotationExtractors(reflect.annotations(fieldSym)).name.getOrElse(name)

          val isFieldAValueType = reflect.isValueClass(fieldTpe)

          if (isFieldAValueType) {

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

            // if the field is a param with a default value, then we know the getter method will be defined
            // and so we can use it to generate the default value. otherwise, we invoke decode without a default
            if (fieldSym.isTerm && fieldSym.asTerm.isParamWithDefault) {

              val defswithsymbols = universe.asInstanceOf[Definitions with SymbolTable with StdNames]
              val defaultGetterName = defswithsymbols.nme.defaultGetterName(defswithsymbols.nme.CONSTRUCTOR, index + 1)
              val defaultGetterMethod = tpe.companion.member(TermName(defaultGetterName.toString))

              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefault[$fieldTpe]($resolvedFieldName, record, $companion.$defaultGetterMethod)"""

            } else {
              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefault[$fieldTpe]($resolvedFieldName, record, null)"""
            }
          }
        }

        c.Expr[Decoder[T]](
          q"""
          new _root_.com.sksamuel.avro4s.Decoder[$tpe] {
            override def decode(value: Any): $tpe = {
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
    * For each field in the target type, we must try to pull a value for that field out
    * of the input GenericRecord. After we retrieve the avro value from the record
    * we must apply the decoder to turn it into a scala compatible value.
    *
    * If the record does not have an entry for a field, and a scala default value is
    * available, then we'll use that instead.
    */
  def decodeFieldOrApplyDefault[T](fieldName: String, record: GenericRecord, default: Any)(implicit decoder: Decoder[T]): T = {
    if (record.getSchema.getField(fieldName) != null) {
      val value = record.get(fieldName)
      decoder.decode(value)
    } else if (default != null) {
      default.asInstanceOf[T]
    } else {
      sys.error(s"Record $record does not have a value for $fieldName and no default was defined")
    }
  }

  def decodeT[T](value: Any)(implicit decoder: Decoder[T]): T = decoder.decode(value)
}
