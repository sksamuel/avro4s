package com.sksamuel.avro4s

import java.io.OutputStream

import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

class DefaultAvroOutputStream[T](os: OutputStream,
                                 serializer: org.apache.avro.io.Encoder)
                                (implicit encoder: Encoder[T]) extends AvroOutputStream[T] {

  private val datumWriter = new GenericDatumWriter[GenericRecord](encoder.schema)

  override def close(): Unit = {
    flush()
    os.close()
  }

  override def write(t: T): Unit = {
    val record = encoder.encode(t).asInstanceOf[GenericRecord]
    datumWriter.write(record, serializer)
  }

  override def flush(): Unit = serializer.flush()
  override def fSync(): Unit = ()
}