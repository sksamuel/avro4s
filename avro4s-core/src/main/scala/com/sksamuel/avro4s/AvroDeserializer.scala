package com.sksamuel.avro4s

import org.apache.avro.generic.GenericRecord
import shapeless._
import shapeless.labelled._

trait Reader[T] {
  def read(name: String, record: GenericRecord): T
}

object Reader {

  implicit object StringReader extends Reader[String] {
    override def read(name: String, record: GenericRecord): String = record.get(name).toString
  }

  implicit object BooleanConverter extends Reader[Boolean] {
    override def read(name: String, record: GenericRecord): Boolean = record.get(name).toString.toBoolean
  }

  implicit object FloatConverter extends Reader[Float] {
    override def read(name: String, record: GenericRecord): Float = record.get(name).toString.toFloat
  }

  implicit object LongConverter extends Reader[Long] {
    override def read(name: String, record: GenericRecord): Long = record.get(name).toString.toLong
  }

  implicit object IntConverter extends Reader[Int] {
    override def read(name: String, record: GenericRecord): Int = record.get(name).toString.toInt
  }

  implicit object DoubleConverter extends Reader[Double] {
    override def read(name: String, record: GenericRecord): Double = record.get(name).toString.toDouble
  }

  //  implicit def OptionConverter[T](implicit converter: AvroConverter[T]) = new AvroConverter[Option[T]] {
  //    override def convert(value: Any): Option[T] = {
  //      Option(value).map(converter.convert)
  //    }
  //  }

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

  implicit object HNilReader extends Reader[HNil] {
    override def read(name: String, record: GenericRecord): HNil = HNil
  }

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
        val v = reader.value.read(key.value.name, record)
        field[K](v) :: remaining(record)
      }
    }
  }

  implicit def GenericSer[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                            deser: AvroDeserializer[Repr]) = new AvroDeserializer[T] {
    override def apply(record: GenericRecord): T = labl.from(deser.apply(record))
  }
}