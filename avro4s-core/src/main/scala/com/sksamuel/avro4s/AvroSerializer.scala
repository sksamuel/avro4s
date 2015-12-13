package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.generic.GenericRecord
import shapeless._
import shapeless.labelled.FieldType
import scala.collection.JavaConverters._

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait Writer[A] {
  def apply(value: A): Any = value
}

object Writer {

  implicit object BooleanWriter extends Writer[Boolean]

  implicit object StringWriter extends Writer[String]

  implicit object DoubleWriter extends Writer[Double]

  implicit object FloatWriter extends Writer[Float]

  implicit object IntWriter extends Writer[Int]

  implicit object LongWriter extends Writer[Long]

  implicit object BigDecimalWriter extends Writer[BigDecimal] {
    override def apply(value: BigDecimal): ByteBuffer = ByteBuffer.wrap(value.toString.getBytes)
  }

  implicit def EitherWriter[T, U](implicit leftWriter: Writer[T], rightWriter: Writer[U]) = new Writer[Either[T, U]] {
    override def apply(value: Either[T, U]): Any = value match {
      case Left(left) => leftWriter.apply(left)
      case Right(right) => rightWriter.apply(right)
    }
  }

  implicit def OptionWriter[T](implicit tovalue: Writer[T]) = new Writer[Option[T]] {
    override def apply(value: Option[T]): Any = value.map(tovalue.apply).orNull
  }

  implicit def ArrayWriter[T](implicit writer: Lazy[Writer[T]]): Writer[Array[T]] = new Writer[Array[T]] {
    override def apply(value: Array[T]): Any = value.headOption match {
      case Some(b: Byte) => ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
      case _ => value.map(writer.value.apply).toSeq.asJavaCollection
    }
  }

  implicit object ByteArrayWriter extends Writer[Array[Byte]] {
    override def apply(value: Array[Byte]): ByteBuffer = ByteBuffer.wrap(value)
  }

  implicit def SetWriter[T](implicit writer: Lazy[Writer[T]]): Writer[Set[T]] = new Writer[Set[T]] {
    override def apply(values: Set[T]): java.util.Collection[Any] = values.map(writer.value.apply).asJavaCollection
  }

  implicit def SeqWriter[T](implicit writer: Lazy[Writer[T]]): Writer[Seq[T]] = new Writer[Seq[T]] {
    override def apply(values: Seq[T]): Any = values.map(writer.value.apply).asJava
  }

  implicit def ListWriter[T](implicit writer: Lazy[Writer[T]]): Writer[List[T]] = new Writer[List[T]] {
    override def apply(values: List[T]): Any = values.map(writer.value.apply).asJava
  }

  implicit def MapWriter[T](implicit writer: Lazy[Writer[T]]) = new Writer[Map[String, T]] {
    override def apply(value: Map[String, T]): java.util.Map[String, T] = {
      value.mapValues(writer.value.apply).asInstanceOf[Map[String, T]].asJava
    }
  }

  implicit def GenericWriter[T](implicit ser: Lazy[AvroSerializer[T]]): Writer[T] = new Writer[T] {
    override def apply(value: T): GenericRecord = ser.value.toRecord(value)
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
                                                         writer: Lazy[Writer[V]],
                                                         remaining: Writes[T],
                                                         tag: ClassTag[V]): Writes[FieldType[Key, V] :: T] = {
    new Writes[FieldType[Key, V] :: T] {
      override def write(record: GenericRecord, value: FieldType[Key, V] :: T): Unit = value match {
        case h :: t =>
          record.put(key.value.name, writer.value(h))
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
                                            schema: Lazy[AvroSchema[T]]) = new AvroSerializer[T] {
    override def toRecord(t: T): GenericRecord = {
      val r = new org.apache.avro.generic.GenericData.Record(schema.value.apply)
      writes.write(r, labl.to(t))
      r
    }
  }

  def apply[T](t: T)(implicit ser: AvroSerializer[T]): AvroSerializer[T] = ser
}