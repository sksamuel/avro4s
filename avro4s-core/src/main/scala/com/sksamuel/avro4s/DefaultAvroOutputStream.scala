package com.sksamuel.avro4s

import java.io.OutputStream

import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

class DefaultAvroOutputStream[T](os: OutputStream,
                                 serializer: org.apache.avro.io.Encoder)
                                (implicit encoder: Encoder[T]) extends AvroOutputStream[T] {

  val resolved = encoder.resolveEncoder()

  private val datumWriter = new GenericDatumWriter[GenericRecord](resolved.schema)

  override def close(): Unit = {
    flush()
    os.close()
  }

  override def write(t: T): Unit = {
    val record = resolved.encode(t).asInstanceOf[GenericRecord]
    datumWriter.write(record, serializer)
  }

  override def flush(): Unit = serializer.flush()
  override def fSync(): Unit = ()
}