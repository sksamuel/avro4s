package com.sksamuel.avro4s

import java.io.OutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

class DefaultAvroOutputStream[T](os: OutputStream,
                                 schema: Schema,
                                 serializer: org.apache.avro.io.Encoder,
                                 fieldMapper: FieldMapper = DefaultFieldMapper)
                                (implicit encoder: Encoder[T]) extends AvroOutputStream[T] {

  private val datumWriter = new GenericDatumWriter[GenericRecord](schema)

  override def close(): Unit = {
    flush()
    os.close()
  }

  override def write(t: T): Unit = {
    val record = encoder.encode(t, schema, fieldMapper).asInstanceOf[GenericRecord]
    datumWriter.write(record, serializer)
  }

  override def flush(): Unit = serializer.flush()
  override def fSync(): Unit = ()
}