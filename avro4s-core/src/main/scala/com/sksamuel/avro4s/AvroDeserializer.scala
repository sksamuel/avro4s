package com.sksamuel.avro4s

import org.apache.avro.generic.GenericRecord
import shapeless._
import shapeless.labelled._

import scala.reflect.ClassTag

trait Reader[T] {
  def read(value: Any): T
}

object Reader {

  implicit object HNilReader extends Reader[HNil] {
    override def read(value: Any): HNil = HNil
  }

  implicit object StringReader extends Reader[String] {
    override def read(value: Any): String = value.toString
  }

  implicit object BooleanReader extends Reader[Boolean] {
    override def read(value: Any): Boolean = value.toString.toBoolean
  }

  implicit object FloatReader extends Reader[Float] {
    override def read(value: Any): Float = value.toString.toFloat
  }

  implicit object LongReader extends Reader[Long] {
    override def read(value: Any): Long = value.toString.toLong
  }

  implicit object IntReader extends Reader[Int] {
    override def read(value: Any): Int = value.toString.toInt
  }

  implicit object DoubleReader extends Reader[Double] {
    override def read(value: Any): Double = value.toString.toDouble
  }

  implicit def OptionReader[T](implicit reader: Reader[T]) = new Reader[Option[T]] {
    override def read(value: Any): Option[T] = Option(value).map(reader.read)
  }

  implicit def ArrayReader[T](implicit reader: Reader[T], tag: ClassTag[T]) = new Reader[Array[T]] {

    import scala.collection.JavaConverters._

    override def read(value: Any): Array[T] = value match {
      case array: Array[T] => array.map(reader.read)
      case list: java.util.Collection[T] => list.asScala.map(reader.read).toArray
    }
  }

  implicit def SeqReader[T](implicit reader: Reader[T]) = new Reader[Seq[T]] {

    import scala.collection.JavaConverters._

    override def read(value: Any): Seq[T] = value match {
      case array: Array[T] => array.map(reader.read)
      case list: java.util.Collection[T] => list.asScala.toSeq.map(reader.read)
    }
  }

  implicit def GenericReader[T](implicit deser: AvroDeserializer[T]) = new Reader[T] {
    override def read(value: Any): T = value match {
      case record: GenericRecord => deser(record)
    }
  }

  //  implicit def EitherConverter[L, R](implicit leftConverter: AvroConverter[L],
  //                                     rightConverter: AvroConverter[R],
  //                                     leftType: ClassTag[L],
  //                                     rightType: ClassTag[R]) = new AvroConverter[Either[L, R]] {
  //    def isMatch[T](tag: ClassTag[T], value: Any) = false
  //    override def convert(value: Any): Either[L, R] = {
  //      val name = value match {
  //        case utf8: Utf8 => classOf[String].getCanonicalName
  //        case true | false => classOf[Boolean].getCanonicalName
  //        case _: Int => classOf[Int].getCanonicalName
  //        case _: Long => classOf[Long].getCanonicalName
  //        case _: Double => classOf[Double].getCanonicalName
  //        case _: Float => classOf[Float].getCanonicalName
  //        case _ => value.getClass.getCanonicalName
  //      }
  //      if (leftType.runtimeClass.getCanonicalName == name) Left(leftConverter.convert(value))
  //      else if (rightType.runtimeClass.getCanonicalName == name) Right(rightConverter.convert(value))
  //      else throw new IllegalArgumentException(s"Value $value of type ${value.getClass} is not compatible with the defined either types")
  //    }
  //  }


}

trait AvroDeserializer[T] {
  def apply(record: GenericRecord): T
}

object AvroDeserializer {

  implicit object HNilDeserializer extends AvroDeserializer[HNil] {
    def apply(record: GenericRecord): HNil = HNil
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       reader: Lazy[Reader[V]],
                                                       remaining: AvroDeserializer[T]): AvroDeserializer[FieldType[K, V] :: T] = {
    new AvroDeserializer[FieldType[K, V] :: T] {

      val fieldName = key.value.name

      override def apply(record: GenericRecord): FieldType[K, V] :: T = {
        val v = reader.value.read(record.get(key.value.name))
        field[K](v) :: remaining(record)
      }
    }
  }

  implicit def GenericSer[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                            deser: AvroDeserializer[Repr]) = new AvroDeserializer[T] {
    override def apply(record: GenericRecord): T = labl.from(deser.apply(record))
  }
}