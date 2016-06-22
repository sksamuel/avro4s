package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import shapeless.Lazy

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

// turns an avro java value into a scala value
trait FromValue[T] {
  def apply(value: Any, field: Field = null): T
}

trait LowPriorityFromValue {
  implicit def generic[T](implicit reader: FromRecord[T]): FromValue[T] = new FromValue[T] {
    override def apply(value: Any, field: Field): T = value match {
      case record: GenericRecord => reader(record)
    }
  }
}

object FromValue extends LowPriorityFromValue {

  implicit object BigDecimalFromValue extends FromValue[BigDecimal] {
    override def apply(value: Any, field: Field): BigDecimal = BigDecimal(new String(value.asInstanceOf[ByteBuffer].array))
  }

  implicit object BooleanFromValue extends FromValue[Boolean] {
    override def apply(value: Any, field: Field): Boolean = value.toString.toBoolean
  }

  implicit object ByteArrayFromValue extends FromValue[Array[Byte]] {
    override def apply(value: Any, field: Field): Array[Byte] = value.asInstanceOf[ByteBuffer].array
  }

  implicit object DoubleFromValue extends FromValue[Double] {
    override def apply(value: Any, field: Field): Double = value.toString.toDouble
  }

  implicit object FloatFromValue extends FromValue[Float] {
    override def apply(value: Any, field: Field): Float = value.toString.toFloat
  }

  implicit object IntFromValue extends FromValue[Int] {
    override def apply(value: Any, field: Field): Int = value.toString.toInt
  }

  implicit object LongFromValue extends FromValue[Long] {
    override def apply(value: Any, field: Field): Long = value.toString.toLong
  }

  implicit object StringFromValue extends FromValue[String] {
    override def apply(value: Any, field: Field): String = value.toString
  }

  implicit object UUIDFromValue extends FromValue[UUID] {
    override def apply(value: Any, field: Field): UUID = UUID.fromString(value.toString)
  }

  implicit def OptionFromValue[T](implicit fromvalue: FromValue[T]) = new FromValue[Option[T]] {
    override def apply(value: Any, field: Field): Option[T] = Option(value).map((value: Any) => fromvalue.apply(value))
  }

  implicit def JavaEnumFromValue[E <: Enum[E]](implicit tag: ClassTag[E]) = new FromValue[E] {
    override def apply(value: Any, field: Field): E = Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], value.toString)
  }

  implicit def ScalaEnumFromValue[E <: Enumeration#Value] = new FromValue[E] {
    override def apply(value: Any, field: Field): E = {
      val klass = Class.forName(field.schema.getFullName + "$")
      import scala.reflect.NameTransformer._
      val enum  = klass.getField(MODULE_INSTANCE_NAME).get(null).asInstanceOf[Enumeration]
      enum.withName(value.toString).asInstanceOf[E]
    }
  }

  implicit def ArrayFromValue[T](implicit fromvalue: FromValue[T],
                                 tag: ClassTag[T]): FromValue[Array[T]] = new FromValue[Array[T]] {
    override def apply(value: Any, field: Field): Array[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value))
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toArray
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def SetFromValue[T](implicit fromvalue: FromValue[T]): FromValue[Set[T]] = new FromValue[Set[T]] {
    override def apply(value: Any, field: Field): Set[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value)).toSet
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toSet
      case other => sys.error("Unsupported set " + other)
    }
  }

  implicit def ListFromValue[T](implicit fromvalue: FromValue[T]): FromValue[List[T]] = new FromValue[List[T]] {
    override def apply(value: Any, field: Field): List[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value)).toList
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toList
      case other => sys.error("Unsupported list " + other)
    }
  }

  implicit def MapFromValue[T](implicit fromvalue: FromValue[T]): FromValue[Map[String, T]] = new FromValue[Map[String, T]] {
    override def apply(value: Any, field: Field): Map[String, T] = value match {
      case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> fromvalue(v) }
      case other => sys.error("Unsupported map " + other)
    }
  }

  implicit def SeqFromValue[T](implicit fromvalue: FromValue[T]): FromValue[Seq[T]] = new FromValue[Seq[T]] {
    override def apply(value: Any, field: Field): Seq[T] = value match {
      case array: Array[_] => array.map((value: Any) => fromvalue.apply(value))
      case list: java.util.Collection[_] => list.asScala.map((value: Any) => fromvalue.apply(value)).toList
      case other => sys.error("Unsupported seq " + other)
    }
  }

  import scala.reflect.runtime.universe.WeakTypeTag

  implicit def EitherFromValue[A, B](implicit
                                     leftfromvalue: FromValue[A],
                                     rightfromvalue: FromValue[B],
                                     leftType: WeakTypeTag[A],
                                     rightType: WeakTypeTag[B]): FromValue[Either[A, B]] = new FromValue[Either[A, B]] {
    override def apply(value: Any, field: Field): Either[A, B] = {

      import scala.reflect.runtime.universe.typeOf

      def convert[C](tpe: scala.reflect.runtime.universe.Type): Either[A, B] = {
        if (leftType.tpe <:< tpe) Left(leftfromvalue(value))
        else if (rightType.tpe <:< tpe) Right(rightfromvalue(value))
        else sys.error(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
      }

      def typeVals(tpe: scala.reflect.runtime.universe.Type): List[String] = {
        tpe.members.filter(_.isTerm).map(_.asTerm).filter(_.isVal).map(_.name.decodedName.toString.trim).toList
      }

      // if we have a generic record, we can't use the type to work out which one it matches,
      // so we have to compare field names
      def fromRecord(record: GenericRecord): Either[A, B] = {
        import scala.collection.JavaConverters._
        val fieldNames = record.getSchema.getFields.asScala.map(_.name).toList
        if (typeVals(leftType.tpe).toSet == fieldNames.toSet) Left(leftfromvalue(value))
        else if (typeVals(rightType.tpe).toSet == fieldNames.toSet) Right(rightfromvalue(value))
        else sys.error(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
      }

      value match {
        case utf8: Utf8 => convert(typeOf[java.lang.String])
        case true | false => convert(typeOf[Boolean])
        case _: Int => convert(typeOf[Int])
        case _: Long => convert(typeOf[Long])
        case _: Double => convert(typeOf[Double])
        case _: Float => convert(typeOf[Float])
        case record: GenericData.Record => fromRecord(record)
        case _ => sys.error(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
      }
    }
  }
}

// converts an avro record into a type T
trait FromRecord[T] extends Serializable {
  def apply(record: org.apache.avro.generic.GenericRecord): T
}

object FromRecord {

  implicit def apply[T]: FromRecord[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[FromRecord[T]] = {
    import c.universe._
    val tpe = weakTypeTag[T].tpe
    require(tpe.typeSymbol.asClass.isCaseClass, s"Require a case class but $tpe is not")

    def fieldsForType(tpe: c.universe.Type): List[c.universe.Symbol] = {
      tpe.decls.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m.paramLists.head
      }.getOrElse(Nil)
    }

    val companion = tpe.typeSymbol.companion

    val converters: Seq[Tree] = fieldsForType(tpe).map { f =>
      val sig = f.typeSignature
      q"""{
          import com.sksamuel.avro4s.ToSchema._
          import com.sksamuel.avro4s.ToValue._
          import com.sksamuel.avro4s.FromValue._

         com.sksamuel.avro4s.FromRecord.converter[$sig] }"""
    }

    val fromValues: Seq[Tree] = fieldsForType(tpe).zipWithIndex.map {
      case (f, idx) =>
        val name = f.name.asInstanceOf[c.TermName]
        val decoded: String = name.decodedName.toString
        val sig = f.typeSignature
        q"""
          {
            val converter = converters($idx).asInstanceOf[com.sksamuel.avro4s.FromValue[$sig]]
            converter(record.get($decoded), record.getSchema.getField($decoded))
          }
        """
    }

    c.Expr[FromRecord[T]](
      q"""new com.sksamuel.avro4s.FromRecord[$tpe] {
            import com.sksamuel.avro4s.ToSchema._
            import com.sksamuel.avro4s.ToValue._
            import com.sksamuel.avro4s.FromValue._

            private val converters = Array(..$converters)

            def apply(record: org.apache.avro.generic.GenericRecord): $tpe = {
              $companion.apply(..$fromValues)
            }
          }
        """
    )
  }

  def converter[T](implicit fromValue: Lazy[FromValue[T]]): FromValue[T] = {
    fromValue.value
  }
}