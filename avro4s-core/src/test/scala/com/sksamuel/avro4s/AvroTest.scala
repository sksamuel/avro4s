package com.sksamuel.avro4s

import java.io.File
import java.nio.file.Paths
import java.util.UUID

import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}

object AvroTest extends App {

  Paths.get("")

  val s = org.apache.avro.SchemaBuilder
    .record("HandshakeRequest").namespace("org.apache.avro.ipc")
    .fields()
    .name("clientHash").`type`().fixed("MD5").size(16).noDefault()
    .name("clientProtocol").`type`().nullable().stringType().noDefault()
    .name("meta").`type`().nullable().map().values().bytesType().noDefault()
    .endRecord()

  println(s)

  val schema = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/students.avsc"))

  val avroFile = new File("students.avro")
  // Create a writer to serialize the record
  val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

  dataFileWriter.create(schema, avroFile)

  val record = new Record(schema)
  record.put("id", UUID.randomUUID.toString)
  record.put("student_id", UUID.randomUUID.toString)
  record.put("university_id", UUID.randomUUID.toString)

  val courseRec = new GenericData.Record(schema.getField("course_details").schema())
  record.put("course_details", courseRec)
  courseRec.put("course_id", UUID.randomUUID.toString)
  courseRec.put("enroll_date", "qwewqe")
  courseRec.put("verb", "qwewqe")
  courseRec.put("result_score", 2.0)

  dataFileWriter.append(record)
  dataFileWriter.close()

  val datumReader = new GenericDatumReader[GenericRecord](schema)
  val dataFileReader = new DataFileReader[GenericRecord](avroFile, datumReader)
  println("Deserialized data is :")
  while (dataFileReader.hasNext) {
    val rec = dataFileReader.next(new GenericData.Record(schema))
    println(rec)
  }
}
