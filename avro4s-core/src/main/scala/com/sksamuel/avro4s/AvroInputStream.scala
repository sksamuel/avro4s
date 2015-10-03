package com.sksamuel.avro4s

import java.io.File
import java.nio.file.Path

import org.apache.avro.file.{DataFileReader, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}

class AvroInputStream[T](in: SeekableInput)(implicit s: AvroSchema[T], pop: AvroPopulator[T]) {

  val datumReader = new GenericDatumReader[GenericRecord](s.schema)
  val dataFileReader = new DataFileReader[GenericRecord](in: SeekableInput, datumReader)

  def iterator: Iterator[T] = {
    new Iterator[T] {

      override def hasNext: Boolean = dataFileReader.hasNext

      override def next(): T = {
        val record = dataFileReader.next(new GenericData.Record(s.schema))
        pop.read(record)
      }
    }
  }

  def close(): Unit = in.close()
}

object AvroInputStream {
  def apply[T: AvroSchema : AvroPopulator](path: String): AvroInputStream[T] = apply(new File(path))
  def apply[T: AvroSchema : AvroPopulator](path: Path): AvroInputStream[T] = apply(path.toFile)
  def apply[T: AvroSchema : AvroPopulator](file: File): AvroInputStream[T] = new AvroInputStream[T](new SeekableFileInput(file))
}