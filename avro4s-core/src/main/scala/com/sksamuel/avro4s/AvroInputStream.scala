package com.sksamuel.avro4s

import java.io.{EOFException, File, InputStream}
import java.nio.file.{Path, Paths}

import org.apache.avro.{AvroTypeException, Schema}
import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DecoderFactory, ResolvingDecoder}

import scala.util.Try

trait AvroInputStream[T] {
  def close(): Unit
  def iterator: Iterator[T]
  def tryIterator: Iterator[Try[T]]
}

class AvroBinaryInputStream[T](in: InputStream, writerSchema: Option[Schema] = None, readerSchema: Option[Schema] = None)
                              (implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {

  private val wSchema = writerSchema.getOrElse(schemaFor())
  private val rSchema = readerSchema.getOrElse(schemaFor())
  private val datumReader = new GenericDatumReader[GenericRecord](wSchema, rSchema)
  private val decoder = DecoderFactory.get().binaryDecoder(in, null)

  private val _iter = Iterator.continually {
    try {
      datumReader.read(null, decoder)
    } catch {
      case _: EOFException => null
    }
  }.takeWhile(_ != null)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = _iter.hasNext
    override def next(): T = fromRecord(_iter.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = _iter.hasNext
    override def next(): Try[T] = Try(fromRecord(_iter.next))
  }

  override def close(): Unit = in.close()
}

class AvroDataInputStream[T](in: SeekableInput, writerSchema: Option[Schema] = None, readerSchema: Option[Schema] = None)
                            (implicit fromRecord: FromRecord[T])
  extends AvroInputStream[T] {
  val datumReader =
    if (writerSchema.isEmpty && readerSchema.isEmpty) new GenericDatumReader[GenericRecord]()
    else if (writerSchema.isDefined && readerSchema.isDefined) new GenericDatumReader[GenericRecord](writerSchema.get, readerSchema.get)
    else if (writerSchema.isDefined) new GenericDatumReader[GenericRecord](writerSchema.get)
    else new GenericDatumReader[GenericRecord](readerSchema.get)
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = fromRecord(dataFileReader.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(fromRecord(dataFileReader.next))
  }

  override def close(): Unit = in.close()
}

class DefaultAwareGenericData extends GenericData {
  override def newRecord(old: scala.Any, schema: Schema): AnyRef = {
    import scala.collection.JavaConverters._
    schema.getFields.asScala.foldLeft(new GenericData.Record(schema)) { case (record, field) =>
      record.put(field.name, field.defaultVal())
      record
    }
  }
}

class DefaultAwareDatumReader[T](writer: Schema, reader: Schema, data: GenericData)
  extends GenericDatumReader[T](writer, reader, data) {
  override def readField(r: scala.Any,
                         f: Schema.Field,
                         oldDatum: scala.Any,
                         in: ResolvingDecoder,
                         state: scala.Any): Unit = {
    try {
      super.readField(r, f, oldDatum, in, state)
    } catch {
      case t: AvroTypeException =>
        if (f.defaultVal == null) throw t else getData.setField(r, f.name, f.pos, f.defaultVal)
    }
  }
}

final case class AvroJsonInputStream[T](in: InputStream, writerSchema: Option[Schema] = None, readerSchema: Option[Schema] = None)
                                       (implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {

  val wSchema = writerSchema.getOrElse(schemaFor())
  val rSchema = readerSchema.getOrElse(schemaFor())
  private val datumReader = new DefaultAwareDatumReader[GenericRecord](wSchema, rSchema, new DefaultAwareGenericData)
  private val jsonDecoder = DecoderFactory.get.jsonDecoder(wSchema, in)

  private def next = Try {
    datumReader.read(null, jsonDecoder)
  }

  def iterator: Iterator[T] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(fromRecord.apply)

  def tryIterator: Iterator[Try[T]] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(record => Try(fromRecord(record)))

  def singleEntity: Try[T] = next.map(fromRecord.apply)

  override def close(): Unit = in.close()
}

object AvroInputStream {

  def json[T: SchemaFor : FromRecord](in: InputStream): AvroJsonInputStream[T] = new AvroJsonInputStream[T](in)
  def json[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroJsonInputStream[T] = json(new SeekableByteArrayInput(bytes))
  def json[T: SchemaFor : FromRecord](file: File): AvroJsonInputStream[T] = json(new SeekableFileInput(file))
  def json[T: SchemaFor : FromRecord](path: String): AvroJsonInputStream[T] = json(Paths.get(path))
  def json[T: SchemaFor : FromRecord](path: Path): AvroJsonInputStream[T] = json(path.toFile)

  def binary[T: SchemaFor : FromRecord](in: InputStream, writerSchema: Schema): AvroBinaryInputStream[T] = new AvroBinaryInputStream[T](in, Option(writerSchema))
  def binary[T: SchemaFor : FromRecord](bytes: Array[Byte], writerSchema: Schema): AvroBinaryInputStream[T] = binary(new SeekableByteArrayInput(bytes), writerSchema)
  def binary[T: SchemaFor : FromRecord](file: File, writerSchema: Schema): AvroBinaryInputStream[T] = binary(new SeekableFileInput(file), writerSchema)
  def binary[T: SchemaFor : FromRecord](path: String, writerSchema: Schema): AvroBinaryInputStream[T] = binary(Paths.get(path), writerSchema)
  def binary[T: SchemaFor : FromRecord](path: Path, writerSchema: Schema): AvroBinaryInputStream[T] = binary(path.toFile, writerSchema)

  // convenience api for cases where the writer schema should be the same as the reader.
  def binary[T: SchemaFor : FromRecord](in: InputStream): AvroBinaryInputStream[T] = new AvroBinaryInputStream[T](in)
  def binary[T: SchemaFor : FromRecord](bytes: Array[Byte]): AvroBinaryInputStream[T] = binary(new SeekableByteArrayInput(bytes))
  def binary[T: SchemaFor : FromRecord](file: File): AvroBinaryInputStream[T] = binary(new SeekableFileInput(file))
  def binary[T: SchemaFor : FromRecord](path: String): AvroBinaryInputStream[T] = binary(Paths.get(path))
  def binary[T: SchemaFor : FromRecord](path: Path): AvroBinaryInputStream[T] = binary(path.toFile)

  def data[T: FromRecord](bytes: Array[Byte]): AvroDataInputStream[T] = new AvroDataInputStream[T](new SeekableByteArrayInput(bytes))
  def data[T: FromRecord](file: File): AvroDataInputStream[T] = new AvroDataInputStream[T](new SeekableFileInput(file))
  def data[T: FromRecord](path: String): AvroDataInputStream[T] = data(Paths.get(path))
  def data[T: FromRecord](path: Path): AvroDataInputStream[T] = data(path.toFile)

  sealed trait AvroFormat {
    def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T]
  }
  object JsonFormat extends AvroFormat {
    override def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T] = new AvroInputStreamBuilderJson[T]()
  }
  object BinaryFormat extends AvroFormat {
    override def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T] = new AvroInputStreamBuilderBinary[T]()
  }
  object DataFormat extends AvroFormat {
    override def newBuilder[T: SchemaFor : FromRecord](): AvroInputStreamBuilder[T] = new AvroInputStreamBuilderData[T]()
  }

  def builder[T: SchemaFor : FromRecord](format: AvroFormat): AvroInputStreamBuilder[T] = format.newBuilder[T]()

  abstract class AvroInputStreamBuilder[T: SchemaFor : FromRecord] {
    protected var writerSchema: Option[Schema] = None
    protected var readerSchema: Option[Schema] = None
    protected var inputStream: InputStream = _
    protected var seekableInput: SeekableInput = _

    def from(path: Path): this.type =
      from(path.toFile)

    def from(path: String): this.type =
      from(Paths.get(path))

    def from(file: File): this.type = {
      val in = new SeekableFileInput(file)
      this.inputStream = in
      this.seekableInput = in
      this
    }

    def from(bytes: Array[Byte]): this.type = {
      val in = new SeekableByteArrayInput(bytes)
      this.inputStream = in
      this.seekableInput = in
      this
    }

    def schema(writerSchema: Schema, readerSchema: Schema): this.type = {
      this.writerSchema = Some(writerSchema)
      this.readerSchema = Some(readerSchema)
      this
    }

    def schema(schema: Schema): this.type =
      this.schema(schema, schema)

    def schemaWriterReader[Writer, Reader]()(implicit writerSchema: SchemaFor[Writer], readerSchema: SchemaFor[Reader]): this.type =
      this.schema(writerSchema(), readerSchema())

    def schema()(implicit schema: SchemaFor[T]): this.type =
      this.schema(schema(), schema())

    def build(): AvroInputStream[T]
  }

  trait FromInputStream {
    this: AvroInputStreamBuilder[_] =>
    def from(in: InputStream): this.type = {
      this.inputStream = in
      this
    }
  }

  class AvroInputStreamBuilderJson[T: SchemaFor : FromRecord] extends AvroInputStreamBuilder[T] with FromInputStream {
    override def build(): AvroInputStream[T] =
      new AvroJsonInputStream[T](inputStream, writerSchema, readerSchema)
  }
  class AvroInputStreamBuilderBinary[T: SchemaFor : FromRecord] extends AvroInputStreamBuilder[T] with FromInputStream {
    override def build(): AvroInputStream[T] =
      new AvroBinaryInputStream[T](inputStream, writerSchema, readerSchema)
  }
  class AvroInputStreamBuilderData[T: SchemaFor : FromRecord] extends AvroInputStreamBuilder[T] {
    override def build(): AvroInputStream[T] =
      new AvroDataInputStream[T](seekableInput, writerSchema, readerSchema)
  }
}