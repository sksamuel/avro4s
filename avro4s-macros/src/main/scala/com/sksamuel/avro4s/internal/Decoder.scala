package com.sksamuel.avro4s.internal

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.{Conversions, LogicalTypes}

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait Decoder[T] extends Serializable {
  self =>

  def decode(value: Any): T

  def map[U](fn: T => U): Decoder[U] = new Decoder[U] {
    override def decode(value: Any): U = fn(self.decode(value))
  }
}

object Decoder {

  implicit object BooleanDecoder extends Decoder[Boolean] {
    override def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object ByteDecoder extends Decoder[Byte] {
    override def decode(value: Any): Byte = value.asInstanceOf[Int].toByte
  }

  implicit object ShortDecoder extends Decoder[Short] {
    override def decode(value: Any): Short = value.asInstanceOf[Int].toShort
  }

  implicit object ByteArrayDecoder extends Decoder[Array[Byte]] {
    override def decode(value: Any): Array[Byte] = value.asInstanceOf[ByteBuffer].array
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
    override def decode(value: Any): Int = value.asInstanceOf[Int]
  }

  implicit object LongDecoder extends Decoder[Long] {
    override def decode(value: Any): Long = value.asInstanceOf[Long]
  }

  implicit object StringDecoder extends Decoder[String] {
    override def decode(value: Any): String = value.toString
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
      case other => sys.error("Unsupported array " + other)
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

  implicit def apply[T]: Decoder[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[Decoder[T]] = {

    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe
    val fullName = tpe.typeSymbol.fullName
    //Console.out.println(s"Require a case clsas but $tpe is not")

    val valueType = reflect.isValueClass(tpe)

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

      val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((fieldSym, fieldTpe), index) =>

        val fieldName = fieldSym.name.asInstanceOf[c.TermName]
        // todo handle avro name annotation
        // val decodedName: Tree = helper.avroName(sym).getOrElse(q"${name.decodedName.toString}")
        val decodedName = q"${fieldName.decodedName.toString}"
        val isFieldAValueType = reflect.isValueClass(fieldTpe)

        if (isFieldAValueType) {

          //  Console.out.println(s"$fieldTpe is a value type")

          val valueCstr = fieldTpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
          val backingFieldTpe = valueCstr.typeSignature

          //  Console.out.println(s"backing field type is $backingFieldTpe")

          // for a value type we first decode the avro type into some scala happy type using an
          // encoder for the backing type, and then we wrap it in an instance of the value class itself
          q"""{
                  val raw = record.get($index)
                  val decoded = _root_.com.sksamuel.avro4s.internal.Decoder.decodeT[$backingFieldTpe](raw) : $backingFieldTpe
                  new $fieldTpe(decoded) : $fieldTpe
            }
         """

        } else {

          q"""{
                  val value = record.get($index)
                  _root_.com.sksamuel.avro4s.internal.Decoder.decodeT[$fieldTpe](value) : $fieldTpe
            }
         """
        }
      }

      // the companion object where the apply construction method is located
      val companion = tpe.typeSymbol.companion

      if (companion == NoSymbol)
        Console.err.println(s"Cannot find companion object for $fullName; If you have defined a local case class, move the definition to a higher enclosing scope.")

      c.Expr[Decoder[T]](
        q"""
          new _root_.com.sksamuel.avro4s.internal.Decoder[$tpe] {
            override def decode(value: Any): $tpe = {
              val record = value.asInstanceOf[_root_.org.apache.avro.generic.IndexedRecord]
              $companion.apply(..$fields)
            }
          }
       """
      )

    }
  }

  def decodeT[T](value: Any)(implicit decoder: Decoder[T]): T = decoder.decode(value)
}
