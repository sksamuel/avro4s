package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.generic.GenericRecord
import shapeless._
import shapeless.labelled.FieldType
import scala.collection.JavaConverters._

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait ToValue[A] {
  def apply(value: A): Option[Any] = Some(value)
}

object ToValue {

  implicit object BooleanToValue extends ToValue[Boolean]

  implicit object StringToValue extends ToValue[String]

  implicit object DoubleToValue extends ToValue[Double]

  implicit object FloatToValue extends ToValue[Float]

  implicit object IntToValue extends ToValue[Int]

  implicit object LongToValue extends ToValue[Long]

  implicit def EitherWriter[T, U](implicit leftWriter: ToValue[T], rightWriter: ToValue[U]) = new ToValue[Either[T, U]] {
    override def apply(value: Either[T, U]): Option[Any] = value match {
      case Left(left) => leftWriter.apply(left)
      case Right(right) => rightWriter.apply(right)
    }
  }

  implicit def OptionToValue[T](implicit tovalue: ToValue[T]) = new ToValue[Option[T]] {
    override def apply(value: Option[T]): Option[Any] = value.flatMap(tovalue.apply)
  }

  implicit def ArrayToValue[T](implicit writer: Lazy[ToValue[T]]): ToValue[Array[T]] = new ToValue[Array[T]] {
    override def apply(value: Array[T]): Option[Any] = value.headOption match {
      case Some(b: Byte) => Some(ByteBuffer.wrap(value.asInstanceOf[Array[Byte]]))
      case _ => Some(value.flatMap(writer.value.apply).toSeq.asJavaCollection)
    }
  }

  implicit object ByteArrayToValue extends ToValue[Array[Byte]] {
    override def apply(value: Array[Byte]): Option[Any] = Some(ByteBuffer.wrap(value))
  }

  implicit def SeqWriter[T](implicit writer: Lazy[ToValue[T]]): ToValue[Seq[T]] = new ToValue[Seq[T]] {
    override def apply(values: Seq[T]): Option[Any] = Some(values.flatMap(writer.value.apply).asJava)
  }

  implicit def ListWriter[T]: ToValue[List[T]] = new ToValue[List[T]] {
    override def apply(values: List[T]): Option[Any] = None
  }

  implicit def MapWriter[T](implicit writer: Lazy[ToValue[T]]) = new ToValue[Map[String, T]] {
    override def apply(value: Map[String, T]): Option[java.util.Map[String, T]] = {
      Some(
        value.mapValues(writer.value.apply).collect {
          case (k, Some(t: T)) => k -> t
        }.asJava
      )
    }
  }

  implicit def GenericToValue[T](implicit ser: Lazy[AvroSerializer[T]]): ToValue[T] = new ToValue[T] {
    override def apply(value: T): Option[GenericRecord] = Some(ser.value.toRecord(value))
  }
}

trait Writes[L <: HList] extends Serializable {
  def write(record: GenericRecord, value: L): Unit
}

object Writes {

  implicit object HNilFields extends Writes[HNil] {
    override def write(record: GenericRecord, value: HNil): Unit = ()
  }

  implicit def HConsFields[Key <: Symbol, V, T <: HList](implicit key: Witness.Aux[Key],
                                                         writer: Lazy[ToValue[V]],
                                                         remaining: Writes[T],
                                                         tag: ClassTag[V]): Writes[FieldType[Key, V] :: T] = {
    new Writes[FieldType[Key, V] :: T] {
      override def write(record: GenericRecord, value: FieldType[Key, V] :: T): Unit = value match {
        case h :: t =>
          writer.value.apply(h).foreach(record.put(key.value.name, _))
          remaining.write(record, t)
      }
    }
  }
}

trait AvroSerializer[T] {
  def toRecord(t: T): GenericRecord
}

object AvroSerializer {

  implicit def GenericSer[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                            writes: Writes[Repr],
                                            schema: Lazy[AvroSchema2[T]]) = new AvroSerializer[T] {
    override def toRecord(t: T): GenericRecord = {
      val r = new org.apache.avro.generic.GenericData.Record(schema.value.apply)
      writes.write(r, labl.to(t))
      r
    }
  }

  def apply[T](t: T)(implicit ser: Lazy[AvroSerializer[T]]): GenericRecord = ser.value.toRecord(t)
}