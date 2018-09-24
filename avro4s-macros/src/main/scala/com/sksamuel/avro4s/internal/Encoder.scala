package com.sksamuel.avro4s.internal

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.{Conversions, LogicalTypes, Schema}

import scala.language.experimental.macros
import scala.math.BigDecimal.RoundingMode

/**
  * Given an instance of T, and a schema, returns an Avro compatible value for the schema.
  * For example, a value of String and a schema of type Schema.Type.FIXED would return a
  * value of Array[Byte].
  */
trait Encoder[T] extends Serializable {
  def encode(t: T, schema: Schema): AnyRef
}

object Encoder {

  implicit def eitherEncoder[T, U](implicit leftEncoder: Encoder[T], rightEncoder: Encoder[U]): Encoder[Either[T, U]] = new Encoder[Either[T, U]] {
    override def encode(t: Either[T, U], schema: Schema): AnyRef = t match {
      case Left(left) => leftEncoder.encode(left, schema.getTypes.get(0))
      case Right(right) => rightEncoder.encode(right, schema.getTypes.get(1))
    }
  }

  implicit object StringEncoder extends Encoder[String] {
    override def encode(t: String, schema: Schema): String = t
  }

  implicit object BooleanEncoder extends Encoder[Boolean] {
    override def encode(t: Boolean, schema: Schema): java.lang.Boolean = java.lang.Boolean.valueOf(t)
  }

  implicit object IntEncoder extends Encoder[Int] {
    override def encode(t: Int, schema: Schema): java.lang.Integer = java.lang.Integer.valueOf(t)
  }

  implicit object LongEncoder extends Encoder[Long] {
    override def encode(t: Long, schema: Schema): java.lang.Long = java.lang.Long.valueOf(t)
  }

  implicit object FloatEncoder extends Encoder[Float] {
    override def encode(t: Float, schema: Schema): java.lang.Float = java.lang.Float.valueOf(t)
  }

  implicit object DoubleEncoder extends Encoder[Double] {
    override def encode(t: Double, schema: Schema): java.lang.Double = java.lang.Double.valueOf(t)
  }

  implicit object ShortEncoder extends Encoder[Short] {
    override def encode(t: Short, schema: Schema): java.lang.Short = java.lang.Short.valueOf(t)
  }

  implicit object ByteEncoder extends Encoder[Byte] {
    override def encode(t: Byte, schema: Schema): java.lang.Byte = java.lang.Byte.valueOf(t)
  }

  implicit object UUIDEncoder extends Encoder[UUID] {
    override def encode(t: UUID, schema: Schema): String = t.toString
  }

  implicit def mapEncoder[V](implicit encoder: Encoder[V]): Encoder[Map[String, V]] = new Encoder[Map[String, V]] {

    import scala.collection.JavaConverters._

    override def encode(map: Map[String, V], schema: Schema): java.util.Map[String, AnyRef] = {
      require(schema != null)
      map.mapValues(encoder.encode(_, schema.getValueType)).asJava
    }
  }

  implicit def listEncoder[T](implicit encoder: Encoder[T]): Encoder[List[T]] = new Encoder[List[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: List[T], schema: Schema): AnyRef = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType)).asJava
    }
  }

  implicit def SetEncoder[T](implicit encoder: Encoder[T]): Encoder[Set[T]] = new Encoder[Set[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Set[T], schema: Schema): AnyRef = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType)).toList.asJava
    }
  }

  implicit def VectorEncoder[T](implicit encoder: Encoder[T]): Encoder[Vector[T]] = new Encoder[Vector[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Vector[T], schema: Schema): AnyRef = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType)).asJava
    }
  }

  implicit def SeqEncoder[T](implicit encoder: Encoder[T]): Encoder[Seq[T]] = new Encoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Seq[T], schema: Schema): AnyRef = {
      require(schema != null)
      ts.map(encoder.encode(_, schema.getElementType)).asJava
    }
  }

  implicit def ArrayEncoder[T](implicit encoder: Encoder[T]): Encoder[Array[T]] = new Encoder[Array[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Array[T], schema: Schema): AnyRef = ts.headOption match {
      case Some(_: Byte) => ByteBuffer.wrap(ts.asInstanceOf[Array[Byte]])
      case _ => ts.map(encoder.encode(_, schema.getElementType)).toList.asJava
    }
  }

  implicit def OptionEncoder[T](implicit encoder: Encoder[T]) = new Encoder[Option[T]] {

    import scala.collection.JavaConverters._

    override def encode(t: Option[T], schema: Schema): AnyRef = {
      // must have a union schema, so we can find the non null part of it
      val nonNullSchema = schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get
      t.map(encoder.encode(_, nonNullSchema)).orNull
    }
  }

  implicit object DecimalEncoder extends Encoder[BigDecimal] {

    override def encode(t: BigDecimal, schema: Schema): ByteBuffer = {

      val decimal = schema.getLogicalType.asInstanceOf[Decimal]
      require(decimal != null)

      val decimalConversion = new Conversions.DecimalConversion
      val decimalType = LogicalTypes.decimal(decimal.getPrecision, decimal.getScale)

      val scaledValue = t.setScale(decimal.getScale, RoundingMode.HALF_UP)
      decimalConversion.toBytes(scaledValue.bigDecimal, null, decimalType)
    }
  }

  implicit def javaEnumEncoder[E <: Enum[_]]: Encoder[E] = new Encoder[E] {
    override def encode(t: E, schema: Schema): EnumSymbol = new EnumSymbol(schema, t.name)
  }

  implicit def scalaEnumEncoder[E <: Enumeration#Value]: Encoder[E] = new Encoder[E] {
    override def encode(t: E, schema: Schema): EnumSymbol = new EnumSymbol(schema, t.toString)
  }

  implicit def apply[T]: Encoder[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[Encoder[T]] = {

    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe
    val annos = reflect.annotations(tpe.typeSymbol)
    val extractor = new AnnotationExtractors(annos)

    val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((f, fieldTpe), index) =>

      val name = f.name.asInstanceOf[c.TermName]
      val annos = reflect.annotations(tpe.typeSymbol)
      val extractor = new AnnotationExtractors(annos)

      // each field needs to be converted into an avro compatible value
      // so scala primitives need to be converted to java boxed values
      // annotations and logical types need to be taken into account

      // if the field is annotated with @AvroFixed then we override the type to be a vector of bytes
      extractor.fixed match {
        case Some(fixed) =>
          q"""{
                val vector = t.$name match {
                  case s: String => s.getBytes("UTF-8").array.toVector
                  case a: Array[Byte] => a.toVector
                  case v: Vector[Byte] => v
                }
                values.append(vector)
              }
           """
        case None =>
          q"""{
                val field = schema.getFields.get($index)
                val value = _root_.com.sksamuel.avro4s.internal.Encoder.doField[$fieldTpe](t.$name : $fieldTpe, field)
                values.append(value)
              }
           """
      }
    }

    c.Expr[Encoder[T]](
      q"""
          new _root_.com.sksamuel.avro4s.internal.Encoder[$tpe] {
            override def encode(t: $tpe, schema: org.apache.avro.Schema): AnyRef = {
              val values = _root_.scala.collection.mutable.ListBuffer.empty[AnyRef]
              ..$fields
              new _root_.com.sksamuel.avro4s.internal.InternalRecord(schema, values.toVector)
            }
          }
       """
    )
  }

  def doField[T](t: T, field: Schema.Field)(implicit encoder: Encoder[T]): AnyRef = {
    encoder.encode(t, field.schema())
  }
}
