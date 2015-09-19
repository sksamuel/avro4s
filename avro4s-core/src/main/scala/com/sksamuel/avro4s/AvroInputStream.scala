package com.sksamuel.avro4s

import org.apache.avro.file.{DataFileReader, SeekableInput}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}

class AvroInputStream[T](in: SeekableInput)(implicit writer: AvroSchema[T]) {

  val datumReader = new GenericDatumReader[GenericRecord](writer.schema)
  val dataFileReader = new DataFileReader[GenericRecord](in: SeekableInput, datumReader)

  def iterator: Iterator[T] = {
    println("Deserialized data is :")
    new Iterator[T] {
      override def hasNext: Boolean = dataFileReader.hasNext
      override def next(): T = dataFileReader.next(new GenericData.Record(writer.schema)).asInstanceOf[T]
    }
  }
}
