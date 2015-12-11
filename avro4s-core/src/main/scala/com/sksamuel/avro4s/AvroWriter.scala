package com.sksamuel.avro4s

import org.apache.avro.generic.GenericRecord
import shapeless._
import shapeless.labelled.FieldType

import scala.language.implicitConversions
import scala.reflect.ClassTag


trait Writer[A] {
  def apply(name: String, value: A, record: GenericRecord): Unit = record.put(name, value)
}

object Writer {

  implicit object StringWriter extends Writer[String]

  implicit object LongWriter extends Writer[Long]

  implicit object IntWriter extends Writer[Int]

  implicit object BooleanWriter extends Writer[Boolean]

  implicit object DoubleWriter extends Writer[Double]

  implicit object FloatWriter extends Writer[Float]

  implicit def EitherWriter[T, U](implicit leftWriter: Writer[T], rightWriter: Writer[U]) = new Writer[Either[T, U]] {
    override def apply(name: String, value: Either[T, U], record: GenericRecord): Unit = value match {
      case Left(left) => leftWriter.apply(name, left, record)
      case Right(right) => rightWriter.apply(name, right, record)
    }
  }

  implicit def OptionWriter[T](implicit twriter: Writer[T]) = new Writer[Option[T]] {
    override def apply(name: String, value: Option[T], record: GenericRecord): Unit = value match {
      case Some(t) => twriter.apply(name, t, record)
      case None => record.put(name, null)
    }
  }

  implicit def ArrayWriter[T] = new Writer[Array[T]] {
    override def apply(name: String, value: Array[T], record: GenericRecord): Unit = {
      import scala.collection.JavaConverters._
      record.put(name, value.toSeq.asJava)
    }
  }

  implicit def SeqWriter[T] = new Writer[Seq[T]] {
    override def apply(name: String, value: Seq[T], record: GenericRecord): Unit = {
      import scala.collection.JavaConverters._
      record.put(name, value.asJava)
    }
  }

  implicit object HNilWriter extends Writer[HNil] {
    override def apply(name: String, value: HNil, record: GenericRecord): Unit = ()
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
                                                         writer: Writer[V],
                                                         remaining: Writes[T],
                                                         tag: ClassTag[V]): Writes[FieldType[Key, V] :: T] = {
    new Writes[FieldType[Key, V] :: T] {
      override def write(record: GenericRecord, value: FieldType[Key, V] :: T): Unit = value match {
        case h :: t =>
          writer(key.value.name, h, record)
          remaining.write(record, t)
      }
    }
  }
}

trait AvroSer[T] {
  def toRecord(t: T): GenericRecord
}

object AvroSer {

  implicit def GenericSer[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                            writes: Writes[Repr],
                                            schema: AvroSchema2[T]) = new AvroSer[T] {
    override def toRecord(t: T): GenericRecord = {
      val r = new org.apache.avro.generic.GenericData.Record(schema())
      writes.write(r, labl.to(t))
      r
    }
  }
}

object Serializer {
  def apply[T](t: T)(implicit ser: AvroSer[T]): GenericRecord = ser.toRecord(t)
}