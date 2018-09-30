package com.sksamuel.avro4s.internal

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, LogicalTypes, Schema}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{:+:, CNil, Coproduct, Generic, HList, Inr}

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

object Decoder {

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

  implicit object ByteSeqDecoder extends Decoder[Seq[Byte]] {
    override def decode(value: Any): Seq[Byte] = value.asInstanceOf[ByteBuffer].array().toSeq
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

  // specialized support for arrays of bytes which are likely to have been serialized as Schema.Type.BYTES
  implicit object ByteArrayDecoder extends Decoder[Array[Byte]] {
    override def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer => buffer.array
      case array: Array[Byte] => array
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

  implicit def genCoproduct[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
                                               coproductDecoder: Decoder[C]): Decoder[T] = new Decoder[T] {
    override def decode(value: Any): T = {
      gen.from(coproductDecoder.decode(value))
    }
  }

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))

  // `apply` here should never be invoked under normal operation; if
  // we're trying to read a value of type CNil it's because we've
  // tried all the other cases and failed. But the FromValue[CNil]
  // needs to exist to supply a base case for the recursion.
  implicit def CNilFromValue: Decoder[CNil] = new Decoder[CNil] {
    override def decode(value: Any): CNil = sys.error("This should never happen: CNil has no inhabitants")
  }

  // We're expecting to read a value of type S :+: T from avro.  Avro
  // unions are untyped, so we have to attempt to read a value of type
  // S (the concrete type), and if that fails, attempt to read the
  // rest of the coproduct type T.

  // thus, the bulk of the logic here is shared with reading Eithers, in `safeFrom`.
  implicit def CoproductFromValue[S: WeakTypeTag : Decoder, T <: Coproduct](implicit decoder: Decoder[T]): Decoder[S :+: T] = new Decoder[S :+: T] {
    override def decode(value: Any): S :+: T = {
      safeFrom[S](value).map(Coproduct[S :+: T](_))
        .getOrElse(Inr(decoder.decode(value)))
    }
  }

  implicit def genTraitObjectEnum[T, C <: Coproduct, L <: HList](implicit gen: Generic.Aux[T, C],
                                                                 objs: Reify.Aux[C, L],
                                                                 toList: ToList[L, T]): Decoder[T] = new Decoder[T] {
    override def decode(value: Any): T = {
      val name = value.toString
      toList(objs()).find(_.toString == name).getOrElse(sys.error(s"Uknown type $name"))
    }
  }

  private def safeFrom[T: WeakTypeTag](value: Any)(implicit decoder: Decoder[T]): Option[T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    def typeName: String = {
      val nearestPackage = Stream.iterate(tpe.typeSymbol.owner)(_.owner).dropWhile(!_.isPackage).head
      s"${nearestPackage.fullName}.${tpe.typeSymbol.name.decodedName}"
    }

    value match {
      case _: Utf8 if tpe <:< typeOf[java.lang.String] => Some(decoder.decode(value))
      case _: String if tpe <:< typeOf[java.lang.String] => Some(decoder.decode(value))
      case true | false if tpe <:< typeOf[Boolean] => Some(decoder.decode(value))
      case _: Int if tpe <:< typeOf[Int] => Some(decoder.decode(value))
      case _: Long if tpe <:< typeOf[Long] => Some(decoder.decode(value))
      case _: Double if tpe <:< typeOf[Double] => Some(decoder.decode(value))
      case _: Float if tpe <:< typeOf[Float] => Some(decoder.decode(value))
      // we don't need to worry about the inner type of the array,
      // as avro schemas will not legally allow multiple arrays in a union
      // tpe is the type we're _expecting_, though, so we need to
      // check both scala and java collections
      case _: GenericData.Array[_]
        if tpe <:< typeOf[Array[_]] ||
          tpe <:< typeOf[java.util.Collection[_]] ||
          tpe <:< typeOf[Iterable[_]] =>
        Some(decoder.decode(value))
      // and similarly for maps
      case _: java.util.Map[_, _]
        if tpe <:< typeOf[java.util.Map[_, _]] ||
          tpe <:< typeOf[Map[_, _]] =>
        Some(decoder.decode(value))
      case record: GenericData.Record if typeName == record.getSchema.getFullName => Some(decoder.decode(value))
      case _ => None
    }
  }

  implicit def applyMacro[T]: Decoder[T] = macro applyMacroImpl[T]

  def applyMacroImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[Decoder[T]] = {

    import c.universe
    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe
    val fullName = tpe.typeSymbol.fullName
    //Console.out.println(s"Require a case clsas but $tpe is not")

    val valueType = reflect.isValueClass(tpe)
    val caseClass = reflect.isCaseClass(tpe)

    if (!caseClass) {
      c.abort(c.enclosingPosition.pos, s"This macro can only encode case classes, not instance of $tpe")
    } else {

      // if we have a value type then we want to return an decoder that decodes
      // the backing field and wraps it in an instance of the value type
      if (valueType) {

        val valueCstr = tpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
        val backingFieldTpe = valueCstr.typeSignature

        c.Expr[Decoder[T]](
          q"""
          new _root_.com.sksamuel.avro4s.internal.Decoder[$tpe] {
            override def decode(value: Any): $tpe = {
              val decoded = _root_.com.sksamuel.avro4s.internal.Decoder.decodeT[$backingFieldTpe](value)
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
          val error = s"Cannot find companion object for $fullName; If you have defined a local case class, move the definition to a higher enclosing scope."
          Console.err.println(error)
          c.abort(c.enclosingPosition.pos, error)
        }

        val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((fieldSym, fieldTpe), index) =>

          val fieldName = fieldSym.name.asInstanceOf[c.TermName]

          // todo handle avro name annotation
          // val decodedName: Tree = helper.avroName(sym).getOrElse(q"${name.decodedName.toString}")
          // val decodedName = q"${fieldName.decodedName.toString}"

          // this is the simple name of the field
          val name = fieldSym.name.decodedName.toString

          val isFieldAValueType = reflect.isValueClass(fieldTpe)

          if (isFieldAValueType) {

            val valueCstr = fieldTpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
            val backingFieldTpe = valueCstr.typeSignature

            // for a value type we first decode the avro type into some scala happy type using an
            // encoder for the backing type, and then we wrap it in an instance of the value class itself
            q"""{
                  val raw = record.get($name)
                  val decoded = _root_.com.sksamuel.avro4s.internal.Decoder.decodeT[$backingFieldTpe](raw) : $backingFieldTpe
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

              q"""_root_.com.sksamuel.avro4s.internal.Decoder.decodeFieldOrApplyDefault[$fieldTpe]($name, record, $companion.$defaultGetterMethod)"""

            } else {
              q"""_root_.com.sksamuel.avro4s.internal.Decoder.decodeFieldOrApplyDefault[$fieldTpe]($name, record, null)"""
            }
          }
        }

        c.Expr[Decoder[T]](
          q"""
          new _root_.com.sksamuel.avro4s.internal.Decoder[$tpe] {
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
    * of the input [[GenericRecord]]. After we retrieve the avro value from the record
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
