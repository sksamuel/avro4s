package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

trait AvroOutputStream[T] {
  def close(): Unit
  def flush(): Unit
  def fSync(): Unit
  def write(t: T): Unit
  def write(ts: Seq[T]): Unit = ts.foreach(write)
}

class AvroBinaryOutputStream[T](os: OutputStream)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T])
  extends AvroOutputStream[T] with StrictLogging {

  val dataWriter = new GenericDatumWriter[GenericRecord](schemaFor())
  val encoder = EncoderFactory.get().binaryEncoder(os, null)

  override def close(): Unit = {
    encoder.flush()
    os.close()
  }

  override def write(t: T): Unit = dataWriter.write(toRecord(t), encoder)
  override def flush(): Unit = encoder.flush()
  override def fSync(): Unit = ()
}

class AvroDataOutputStream[T](os: OutputStream)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T])
  extends AvroOutputStream[T] with StrictLogging {

  val datumWriter = new GenericDatumWriter[GenericRecord](schemaFor())
  val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(schemaFor(), os)

  override def close(): Unit = {
    dataFileWriter.close()
    os.close()
  }

  override def write(t: T): Unit = dataFileWriter.append(toRecord(t))
  override def flush(): Unit = dataFileWriter.flush()
  override def fSync(): Unit = dataFileWriter.fSync()
}

object AvroOutputStream {
  def apply[T: SchemaFor : ToRecord](file: File): AvroOutputStream[T] = apply(file.toPath, true)

  def apply[T: SchemaFor : ToRecord](file: File, includeSchema: Boolean): AvroOutputStream[T] = apply(file.toPath, includeSchema)

  def apply[T: SchemaFor : ToRecord](path: Path): AvroOutputStream[T] = apply(Files.newOutputStream(path), true)

  def apply[T: SchemaFor : ToRecord](path: Path, includeSchema: Boolean): AvroOutputStream[T] = apply(Files.newOutputStream(path), includeSchema)

  def apply[T: SchemaFor : ToRecord](os: OutputStream): AvroOutputStream[T] = apply(os, true)

  def apply[T: SchemaFor : ToRecord](os: OutputStream, includeSchema: Boolean): AvroOutputStream[T] = {
    if (includeSchema) new AvroDataOutputStream[T](os)
    else new AvroBinaryOutputStream[T](os)
  }
}