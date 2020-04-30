package com.sksamuel.avro4s

import java.io.OutputStream

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

/**
  * An [[AvroOutputStream]] that writes the schema along with the messages.
  * This is usually the format required when writing multiple messages to a single file.
  * Some frameworks, such as a Kafka, store the Schema separately to messages, in which
  * case the [[AvroBinaryInputStream]] is what you would need.
  *
  * @param os      the underlying stream that data will be written to.
  * @param codec   compression codec
  * @param encoder the avro4s [[Encoder]] that will convert each value to a GenericRecord.
  */
case class AvroDataOutputStream[T](os: OutputStream,
                                   codec: CodecFactory)
                                  (implicit encoder: Encoder[T]) extends AvroOutputStream[T] {

  val (writer, writeFn) = encoder.schema.getType match {
    case Schema.Type.DOUBLE | Schema.Type.LONG | Schema.Type.BOOLEAN | Schema.Type.STRING | Schema.Type.INT | Schema.Type.FLOAT =>
      val datumWriter = new GenericDatumWriter[T](encoder.schema)
      val dataFileWriter = new DataFileWriter[T](datumWriter)
      dataFileWriter.setCodec(codec)
      dataFileWriter.create(encoder.schema, os)
      (dataFileWriter, (t: T) => dataFileWriter.append(t))
    case _ =>
      val datumWriter = new GenericDatumWriter[GenericRecord](encoder.schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.setCodec(codec)
      dataFileWriter.create(encoder.schema, os)
      (dataFileWriter, (t: T) => {
        val record = encoder.encode(t).asInstanceOf[GenericRecord]
        dataFileWriter.append(record)
      })
  }

  override def close(): Unit = {
    flush()
    writer.close()
  }

  override def write(t: T): Unit = {
    writeFn(t)
  }

  override def flush(): Unit = writer.flush()
  override def fSync(): Unit = writer.fSync()
}