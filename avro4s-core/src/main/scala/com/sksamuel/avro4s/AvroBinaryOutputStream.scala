package com.sksamuel.avro4s

import java.io.OutputStream

import com.sksamuel.avro4s.internal.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

/**
  * An [[AvroOutputStream]] that does not write the [[org.apache.avro.Schema]]. Use this when
  * you want the smallest messages possible at the cost of not having the schema available
  * in the messages for downstream clients.
  */
case class AvroBinaryOutputStream[T](os: OutputStream, schema: Schema, encoder: Encoder[T])
  extends AvroOutputStream[T] {

  private val dataWriter = new GenericDatumWriter[GenericRecord](schema)
  private val binaryEncoder = EncoderFactory.get().binaryEncoder(os, null)

  override def close(): Unit = {
    flush()
    os.close()
  }

  override def write(t: T): Unit = {
    val record = encoder.encode(t, schema).asInstanceOf[GenericRecord]
    dataWriter.write(record, binaryEncoder)
  }

  override def flush(): Unit = binaryEncoder.flush()
  override def fSync(): Unit = ()
  override def close(closeUnderlying: Boolean): Unit = ???
}