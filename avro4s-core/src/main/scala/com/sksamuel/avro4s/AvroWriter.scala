package com.sksamuel.avro4s

import java.nio.file.{Files, Paths}

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import shapeless._
import shapeless.ops.hlist.Mapper
import shapeless.syntax._
import shapeless.record._
import shapeless.labelled.FieldType
import shapeless.ops.record.{Fields, Keys, Values}

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait FieldWrite[T] {
  def write(name: String, value: T, record: Record): Unit = record.put(name, value)
}

object FieldWrite {

  implicit object StringFieldWrite extends FieldWrite[String]

  implicit object BooleanFieldWrite extends FieldWrite[Boolean]

  implicit object HNilFieldWrite extends FieldWrite[HNil] {
    override def write(name: String, value: HNil, record: Record): Unit = ()
  }

  implicit def HConsFieldWrite[H, T <: HList](implicit hw: FieldWrite[H],
                                              tw: FieldWrite[T]): FieldWrite[H :: T] = new FieldWrite[H :: T] {
    override def write(name: String, value: H :: T, record: Record): Unit = value match {
      case h :: t =>
        hw.write(name, h, record)
        tw.write(name, t, record)
    }
  }

  implicit def GenericFieldWrite[T, Repr <: HList](implicit gen: LabelledGeneric.Aux[T, Repr],
                                                   kk: Keys[Repr],
                                                   vv: Values[Repr],
                                                   ff: Fields[Repr],
                                                   rw: Lazy[FieldWrite[Repr]],
                                                   s: AvroSchema[T]): FieldWrite[T] = new FieldWrite[T] {
    override def write(name: String, value: T, record: Record): Unit = {
      val r = new org.apache.avro.generic.GenericData.Record(s.schema)
      rw.value.write(name, gen.to(value), r)
      record.put(name, r)
    }
  }
}

/**
  * AvroSerializer is a starting point to create a serialized form of a value T.
  * It is the helper that will invoke the recursive FieldWrite typeclasses.
  */
trait AvroSerializer2[T] {
  def serialize(t: T): GenericRecord
}

object ToName extends Poly1 {
  implicit def all[A] = at[Symbol with A](_.name)
}

object FieldMappings extends Poly1 {
  implicit def all[A] = at[Symbol with A](x => println(x))
}

object AvroSerializer2 {
  implicit def serializer[T, Repr <: HList](implicit s: AvroSchema[T],
                                            labl: LabelledGeneric.Aux[T, Repr],
                                            kk: Keys[Repr],
                                            vv: Values[Repr],
                                            ff: Fields[Repr]): AvroSerializer2[T] = new AvroSerializer2[T] {
    override def serialize(t: T): GenericRecord = {

      val r = new org.apache.avro.generic.GenericData.Record(s.schema)

      val keys = kk.apply()
      val values = vv.apply(labl.to(t))
      val fields = ff.apply(labl.to(t))

      println(keys)
      println(values)
      println(fields)

      r.put("name", "sammy")
      r.put("cool", true)
      r
    }
  }
}


trait FieldTags[L <: HList] extends DepFn0 with Serializable {
  type Out = List[ClassTag[_]]
}

object FieldTags extends App {

  def apply[L <: HList](implicit fields: FieldTags[L]): FieldTags[L] = fields

  implicit def hnilFields[L <: HNil]: FieldTags[L] = new FieldTags[L] {
    def apply() = List.empty
  }

  implicit def hconsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       tailFields: FieldTags[T],
                                                       write: FieldWrite[V],
                                                       tag: ClassTag[V]): FieldTags[FieldType[K, V] :: T] = {
    new FieldTags[FieldType[K, V] :: T] {
      def apply() = {
        tag :: tailFields.apply()
      }
    }
  }

}


object Write extends App {

  import shapeless._
  import shapeless.syntax.singleton._

  println("qweqw".narrow)

  val labl = LabelledGeneric[Foo]
  val keys = Keys[labl.Repr].apply

  val things = keys.map(MyFunc)
  println("things=" + things)

  val fields: List[ClassTag[_]] = FieldTags[labl.Repr].apply()
  println(fields)

  import scala.reflect.classTag

  val str = classOf[java.lang.String]
  val b = classOf[java.lang.Boolean]
  val bb = classOf[Boolean]

  fields foreach { x => x.runtimeClass match {
    case `str` => println("I am a string")
    case `b` | `bb` => println("boolean")
    case other => println("I am an another: " + other.getClass)
  }
  }

  import AvroImplicits._

  def write[T](t: T)(implicit s: AvroSchema[T], serializer: AvroSerializer2[T]): Unit = {

    val os = Files.newOutputStream(Paths.get("test.avro"))

    val datumWriter = new GenericDatumWriter[GenericRecord](s.schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(s.schema, os)

    val record = serializer.serialize(t)
    println(record)
    dataFileWriter.append(record)

    dataFileWriter.flush()
    dataFileWriter.close()
  }

  write(Bibble("sammy", true))
}

case class Bibble(name: String, cool: Boolean)
