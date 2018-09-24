package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.sksamuel.avro4s.internal.Encoder
import org.apache.avro.Schema

//import java.io.{File, OutputStream}
//import java.nio.file.{Files, Path}
//
//import org.apache.avro.Schema
//import org.apache.avro.file.{CodecFactory, DataFileWriter}
//import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
//import org.apache.avro.io.EncoderFactory

/**
  * An [[AvroOutputStream]] will write instances of T to an underlying
  * representation.
  *
  * There are three implementations of this stream
  *  - a Data stream,
  *  - a BB stream
  *  - a Json stream
  *
  * See the methods on the companion object to create instances of each
  * of these types of stream.
  */
trait AvroOutputStream[T] {
  def close(): Unit
  def close(closeUnderlying: Boolean): Unit
  def flush(): Unit
  def fSync(): Unit
  def write(t: T): Unit
  def write(ts: Seq[T]): Unit = ts.foreach(write)
}

object AvroOutputStream {
  // convenience api for cases where the user wants to use the default codec.
  def data[T: Encoder](file: File, schema: Schema): AvroDataOutputStream[T] = data(file.toPath, schema)
  def data[T: Encoder](path: Path, schema: Schema): AvroDataOutputStream[T] = data(Files.newOutputStream(path), schema)
  def data[T: Encoder](os: OutputStream, schema: Schema): AvroDataOutputStream[T] = AvroDataOutputStream(os, schema)
}

//// avro output stream that does not write the schema, only use when you want the smallest messages possible
//// at the cost of not having the schema available in the messages for downstream clients
//case class AvroBinaryOutputStream[T](os: OutputStream)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T])
//  extends AvroOutputStream[T] {
//
//  val dataWriter = new GenericDatumWriter[GenericRecord](schemaFor())
//  val encoder = EncoderFactory.get().binaryEncoder(os, null)
//
//  override def close(): Unit = close(true)
//  override def close(closeUnderlying: Boolean): Unit = {
//    flush()
//    if (closeUnderlying)
//      os.close()
//  }
//
//  override def write(t: T): Unit = dataWriter.write(toRecord(t), encoder)
//  override def flush(): Unit = encoder.flush()
//  override def fSync(): Unit = ()
//}
//

//
//// avro output stream that writes json instead of a packed format
//case class AvroJsonOutputStream[T](os: OutputStream)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T])
//  extends AvroOutputStream[T] {
//
//  private val schema = schemaFor()
//  protected val datumWriter = new GenericDatumWriter[GenericRecord](schema)
//  private val encoder = EncoderFactory.get.jsonEncoder(schema, os)
//
//  override def close(): Unit = close(true)
//  override def close(closeUnderlying: Boolean): Unit = {
//    flush()
//    if (closeUnderlying)
//      os.close()
//  }
//
//  override def write(t: T): Unit = datumWriter.write(toRecord(t), encoder)
//  override def fSync(): Unit = {}
//  override def flush(): Unit = encoder.flush()
//}
//
//object AvroOutputStream {
//
//  def json[T: SchemaFor : ToRecord](file: File): AvroJsonOutputStream[T] = json(file.toPath)
//  def json[T: SchemaFor : ToRecord](path: Path): AvroJsonOutputStream[T] = json(Files.newOutputStream(path))
//  def json[T: SchemaFor : ToRecord](os: OutputStream): AvroJsonOutputStream[T] = AvroJsonOutputStream(os)
//
//  def data[T: SchemaFor : ToRecord](file: File, codec: CodecFactory): AvroDataOutputStream[T] = data(file.toPath, codec)
//  def data[T: SchemaFor : ToRecord](path: Path, codec: CodecFactory): AvroDataOutputStream[T] = data(Files.newOutputStream(path), codec)
//  def data[T: SchemaFor : ToRecord](os: OutputStream, codec: CodecFactory): AvroDataOutputStream[T] = AvroDataOutputStream(os, codec)
//

//
//  def binary[T: SchemaFor : ToRecord](file: File): AvroBinaryOutputStream[T] = binary(file.toPath)
//  def binary[T: SchemaFor : ToRecord](path: Path): AvroBinaryOutputStream[T] = binary(Files.newOutputStream(path))
//  def binary[T: SchemaFor : ToRecord](os: OutputStream): AvroBinaryOutputStream[T] = AvroBinaryOutputStream(os)
//}