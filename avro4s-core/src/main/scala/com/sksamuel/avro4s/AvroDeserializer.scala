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