package com.sksamuel.avro4s

import java.io.OutputStream

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.GenericDatumWriter

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
    case Schema.Type.BOOLEAN | Schema.Type.INT | Schema.Type.LONG | Schema.Type.FLOAT | Schema.Type.DOUBLE | Schema.Type.STRING =>
      val datumWriter = new GenericDatumWriter[T](encoder.schema)
      val dataFileWriter = new DataFileWriter[T](datumWriter)
      dataFileWriter.setCodec(codec)
      dataFileWriter.create(encoder.schema, os)
      // No encoding needed for these primitive types
      (dataFileWriter, (t: T) => dataFileWriter.append(t))
    case _ => // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, BYTES, NULL
      val datumWriter = new GenericDatumWriter[AnyRef](encoder.schema)
      val dataFileWriter = new DataFileWriter[AnyRef](datumWriter)
      dataFileWriter.setCodec(codec)
      dataFileWriter.create(encoder.schema, os)
      (dataFileWriter, (t: T) => {
        val encoded = encoder.encode(t)
        dataFileWriter.append(encoded)
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