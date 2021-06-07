package com.sksamuel.avro4s

import org.apache.avro.Schema

import java.io.OutputStream
import org.apache.avro.generic.GenericDatumWriter

class AvroBinaryOutputStream[T](schema: Schema,
                                os: OutputStream,
                                serializer: org.apache.avro.io.Encoder)
                               (using encoder: Encoder[T]) extends AvroOutputStream[T] {

  private val datumWriter = new GenericDatumWriter[Any](schema)
  private val encodeT = encoder.encode(schema)

  override def close(): Unit = {
    flush()
    os.close()
  }

  override def write(t: T): Unit = {
    val datum = encodeT(t)
    datumWriter.write(datum, serializer)
  }

  override def flush(): Unit = serializer.flush()
  override def fSync(): Unit = ()
}