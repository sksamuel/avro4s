package com.sksamuel.avro4s

import java.io.{File, OutputStream}
import java.nio.file.{Files, Path}

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

trait AvroOutputStream[T] {
  def close(): Unit
  def close(closeUnderlying: Boolean): Unit
  def flush(): Unit
  def fSync(): Unit
  def write(t: T): Unit
  def write(ts: Seq[T]): Unit = ts.foreach(write)
}

// avro output stream that does not write the schema, only use when you want the smallest messages possible
// at the cost of not having the schema available in the messages for downstream clients
case class AvroBinaryOutputStream[T](os: OutputStream)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T])
  extends AvroOutputStream[T] {

  val dataWriter = new GenericDatumWriter[GenericRecord](schemaFor())
  val encoder = EncoderFactory.get().binaryEncoder(os, null)

  override def close(): Unit = close(true)
  override def close(closeUnderlying: Boolean): Unit = {
    flush()
    if (closeUnderlying)
      os.close()
  }

  override def write(t: T): Unit = dataWriter.write(toRecord(t), encoder)
  override def flush(): Unit = encoder.flush()
  override def fSync(): Unit = ()
}

// avro output stream that includes the schema for the messages. This is usually what you want.
case class AvroDataOutputStream[T](os: OutputStream, codec: CodecFactory = CodecFactory.nullCodec())(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T])
  extends AvroOutputStream[T] {

  val schema = schemaFor()
  val (writer, writeFn) = schema.getType match {
    case Schema.Type.DOUBLE | Schema.Type.LONG | Schema.Type.BOOLEAN | Schema.Type.STRING | Schema.Type.INT | Schema.Type.FLOAT =>
      val datumWriter = new GenericDatumWriter[T](schema)
      val dataFileWriter = new DataFileWriter[T](datumWriter)
      dataFileWriter.setCodec(codec)
      dataFileWriter.create(schema, os)
      (dataFileWriter, (t: T) => dataFileWriter.append(t))
    case _ =>
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.setCodec(codec)
      dataFileWriter.create(schema, os)
      (dataFileWriter, (t: T) => {
        val record = toRecord.apply(t)
        dataFileWriter.append(record)
      })
  }

  override def close(): Unit = close(true)
  override def close(closeUnderlying: Boolean): Unit = {
    flush()
    writer.close()
    if (closeUnderlying)
      os.close()
  }

  override def write(t: T): Unit = {
    writeFn(t)
  }
  override def flush(): Unit = writer.flush()
  override def fSync(): Unit = writer.fSync()
}

// avro output stream that writes json instead of a packed format
case class AvroJsonOutputStream[T](os: OutputStream)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T])
  extends AvroOutputStream[T] {

  private val schema = schemaFor()
  protected val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  private val encoder = EncoderFactory.get.jsonEncoder(schema, os)

  override def close(): Unit = close(true)
  override def close(closeUnderlying: Boolean): Unit = {
    flush()
    if (closeUnderlying)
      os.close()
  }

  override def write(t: T): Unit = datumWriter.write(toRecord(t), encoder)
  override def fSync(): Unit = {}
  override def flush(): Unit = encoder.flush()
}

object AvroOutputStream {

  def json[T: SchemaFor : ToRecord](file: File): AvroJsonOutputStream[T] = json(file.toPath)
  def json[T: SchemaFor : ToRecord](path: Path): AvroJsonOutputStream[T] = json(Files.newOutputStream(path))
  def json[T: SchemaFor : ToRecord](os: OutputStream): AvroJsonOutputStream[T] = AvroJsonOutputStream(os)

  def data[T: SchemaFor : ToRecord](file: File, codec: CodecFactory): AvroDataOutputStream[T] = data(file.toPath, codec)
  def data[T: SchemaFor : ToRecord](path: Path, codec: CodecFactory): AvroDataOutputStream[T] = data(Files.newOutputStream(path), codec)
  def data[T: SchemaFor : ToRecord](os: OutputStream, codec: CodecFactory): AvroDataOutputStream[T] = AvroDataOutputStream(os, codec)

  // convenience api for cases where the the user want to use the default null codec.
  def data[T: SchemaFor : ToRecord](file: File): AvroDataOutputStream[T] = data(file.toPath)
  def data[T: SchemaFor : ToRecord](path: Path): AvroDataOutputStream[T] = data(Files.newOutputStream(path))
  def data[T: SchemaFor : ToRecord](os: OutputStream): AvroDataOutputStream[T] = AvroDataOutputStream(os)

  def binary[T: SchemaFor : ToRecord](file: File): AvroBinaryOutputStream[T] = binary(file.toPath)
  def binary[T: SchemaFor : ToRecord](path: Path): AvroBinaryOutputStream[T] = binary(Files.newOutputStream(path))
  def binary[T: SchemaFor : ToRecord](os: OutputStream): AvroBinaryOutputStream[T] = AvroBinaryOutputStream(os)
}