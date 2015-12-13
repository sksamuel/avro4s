package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import shapeless._
import shapeless.labelled._

import scala.reflect.ClassTag

trait FromValue[T] {
  def apply(value: Any): T
}

object FromValue {

  implicit object HNilFromValue extends FromValue[HNil] {
    override def apply(value: Any): HNil = HNil
  }

  implicit object BigDecimalFromValue extends FromValue[BigDecimal] {
    override def apply(value: Any): BigDecimal = BigDecimal(new String(value.asInstanceOf[ByteBuffer].array))
  }

  implicit object BooleanFromValue extends FromValue[Boolean] {
    override def apply(value: Any): Boolean = value.toString.toBoolean
  }

  implicit object DoubleFromValue extends FromValue[Double] {
    override def apply(value: Any): Double = value.toString.toDouble
  }

  implicit object FloatFromValue extends FromValue[Float] {
    override def apply(value: Any): Float = value.toString.toFloat
  }

  implicit object IntFromValue extends FromValue[Int] {
    override def apply(value: Any): Int = value.toString.toInt
  }

  implicit object LongFromValue extends FromValue[Long] {
    override def apply(value: Any): Long = value.toString.toLong
  }

  implicit object StringFromValue extends FromValue[String] {
    override def apply(value: Any): String = value.toString
  }

  implicit def OptionReader[T](implicit fromvalue: FromValue[T]) = new FromValue[Option[T]] {
    override def apply(value: Any): Option[T] = Option(value).map(fromvalue.apply)
  }

  implicit def ArrayReader[T](implicit fromvalue: FromValue[T],
                              tag: ClassTag[T]): FromValue[Array[T]] = new FromValue[Array[T]] {

    import scala.collection.JavaConverters._

    override def apply(value: Any): Array[T] = value match {
      case array: Array[T] => array.map(fromvalue.apply)
      case list: java.util.Collection[T] => list.asScala.map(fromvalue.apply).toArray
    }
  }

  implicit def SetReader[T](implicit fromvalue: FromValue[T]): FromValue[Set[T]] = new FromValue[Set[T]] {

    import scala.collection.JavaConverters._

    override def apply(value: Any): Set[T] = value match {
      case array: Array[T] => array.map(fromvalue.apply).toSet
      case list: java.util.Collection[T] => list.asScala.map(fromvalue.apply).toSet
    }
  }

  implicit def ListReader[T](implicit fromvalue: FromValue[T]): FromValue[List[T]] = new FromValue[List[T]] {

    import scala.collection.JavaConverters._

    override def apply(value: Any): List[T] = value match {
      case array: Array[T] => array.map(fromvalue.apply).toList
      case list: java.util.Collection[T] => list.asScala.map(fromvalue.apply).toList
    }
  }

  implicit def SeqReader[T](implicit fromvalue: FromValue[T]): FromValue[Seq[T]] = new FromValue[Seq[T]] {

    import scala.collection.JavaConverters._

    override def apply(value: Any): Seq[T] = value match {
      case array: Array[T] => array.map(fromvalue.apply)
      case list: java.util.Collection[T] => list.asScala.map(fromvalue.apply).toList
    }
  }

  implicit def MapReader[T](implicit fromvalue: FromValue[T]): FromValue[Map[String, T]] = new FromValue[Map[String, T]] {

    import scala.collection.JavaConverters._

    override def apply(value: Any): Map[String, T] = value match {
      case map: java.util.Map[Any, Any] =>
        map.asScala.toMap.map { case (k, v) => k.toString -> fromvalue(v) }
    }
  }

  implicit def GenericReader[T](implicit reader: Lazy[AvroReader[T]]): FromValue[T] = new FromValue[T] {
    override def apply(value: Any): T = value match {
      case record: GenericRecord => reader.value(record)
    }
  }

  import scala.reflect.runtime.universe.WeakTypeTag

  implicit def EitherReader[A, B](implicit
                                  leftfromvalue: FromValue[A],
                                  rightfromvalue: FromValue[B],
                                  leftType: WeakTypeTag[A],
                                  rightType: WeakTypeTag[B]): FromValue[Either[A, B]] = new FromValue[Either[A, B]] {
    override def apply(value: Any): Either[A, B] = {

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
        case utf8: Utf8 => convert(typeOf[String])
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

trait AvroReader[T] {
  def apply(record: GenericRecord): T
}

object AvroReader {

  implicit object HNilReader extends AvroReader[HNil] {
    def apply(record: GenericRecord): HNil = HNil
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       fromValue: Lazy[FromValue[V]],
                                                       remaining: AvroReader[T]): AvroReader[FieldType[K, V] :: T] = {
    new AvroReader[FieldType[K, V] :: T] {

      val fieldName = key.value.name

      override def apply(record: GenericRecord): FieldType[K, V] :: T = {
        val v = fromValue.value(record.get(key.value.name))
        field[K](v) :: remaining(record)
      }
    }
  }

  implicit def GenericSer[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                            reader: Lazy[AvroReader[Repr]]) = new AvroReader[T] {
    override def apply(record: GenericRecord): T = labl.from(reader.value(record))
  }

  def apply[T](implicit reader: Lazy[AvroReader[T]]): AvroReader[T] = reader.value
}