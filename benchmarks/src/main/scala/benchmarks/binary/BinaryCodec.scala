package benchmarks.binary

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.Instant

import benchmarks.binary.BinaryCodec.ValidIntBinaryCodec.codecs
import benchmarks.record.{AttributeValue, RecordWithUnionAndTypeField}
import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.io._
import org.apache.avro.util.ReusableByteBufferInputStream

import scala.collection.mutable

trait BinaryCodec[T] {

  def read(decoder: ResolvingDecoder): T

  def write(value: T, encoder: Encoder): Unit

  def schema: Schema

  val map = ThreadLocal.withInitial[mutable.Map[Schema, ResolvingDecoder]](() => new mutable.HashMap())

  def readBytes(bytes: ByteBuffer, writerSchema: Schema): T = {
    val in = new ReusableByteBufferInputStream()
    in.setByteBuffer(bytes)
    val binary = DecoderFactory.get().directBinaryDecoder(in, null)
    val localMap = map.get()
    val decoder = localMap.get(writerSchema) match {
      case Some(d) => d
      case None =>
        val d = DecoderFactory.get.resolvingDecoder(Schema.applyAliases(writerSchema, schema), schema, null)
        localMap.put(writerSchema, d)
        d
    }
    decoder.configure(binary)
    read(decoder)
  }

  def writeBytes(value: T): ByteBuffer = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.directBinaryEncoder(out, null)
    write(value, encoder)
    ByteBuffer.wrap(out.toByteArray)
  }
}

object BinaryCodec {

  object IntBinaryCodec extends BinaryCodec[Int] {
    def read(decoder: ResolvingDecoder): Int = decoder.readInt


    def write(value: Int, encoder: Encoder): Unit = encoder.writeInt(value)

    val schema: Schema = SchemaBuilder.builder.intType
  }

  object InstantBinaryCodec extends BinaryCodec[Instant] {
    def read(decoder: ResolvingDecoder): Instant = Instant.ofEpochMilli(decoder.readLong)

    val schema: Schema = AvroSchema[Instant]

    def write(value: Instant, encoder: Encoder): Unit = encoder.writeLong(value.toEpochMilli)
  }

  object ValidIntBinaryCodec extends BinaryCodec[AttributeValue.Valid[Int]] {
    val codecs = Array[BinaryCodec[_]](IntBinaryCodec, InstantBinaryCodec)

    val schema: Schema = AvroSchema[AttributeValue.Valid[Int]]

    def read(decoder: ResolvingDecoder): AttributeValue.Valid[Int] = {
      val values = readFields(codecs, decoder)
      AttributeValue.Valid(values(0).asInstanceOf[Int], values(1).asInstanceOf[Instant])
    }

    def write(value: AttributeValue.Valid[Int], encoder: Encoder): Unit = {
      codecs(0).asInstanceOf[BinaryCodec[Int]].write(value.value, encoder)
      codecs(1).asInstanceOf[BinaryCodec[Instant]].write(value.timestamp, encoder)
    }
  }

  object IntAttributeBinaryCodec extends BinaryCodec[AttributeValue[Int]] {
    def read(decoder: ResolvingDecoder): AttributeValue[Int] = {
      decoder.readIndex match {
        case 2 => ValidIntBinaryCodec.read(decoder)
        case _ => ???
      }
    }


    def write(value: AttributeValue[Int], encoder: Encoder): Unit = {
      value match {
        case valid: AttributeValue.Valid[Int] =>
          encoder.writeIndex(2)
          ValidIntBinaryCodec.write(valid, encoder)
        case _ => ???
      }

    }

    val schema: Schema = AvroSchema[AttributeValue[Int]]
  }

  object RecordWithUnionAndTypeFieldBinaryCodec extends BinaryCodec[RecordWithUnionAndTypeField] {
    val codecs = Array[BinaryCodec[_]](IntAttributeBinaryCodec)
    def read(decoder: ResolvingDecoder): RecordWithUnionAndTypeField = {
      val values = readFields(codecs, decoder)
      RecordWithUnionAndTypeField(values(0).asInstanceOf[AttributeValue.Valid[Int]])
    }


    def write(value: RecordWithUnionAndTypeField, encoder: Encoder): Unit = {
      codecs(0).asInstanceOf[BinaryCodec[AttributeValue[Int]]].write(value.attribute, encoder)
    }

    val schema: Schema = AvroSchema[RecordWithUnionAndTypeField]
  }

  def readFields(readers: Array[BinaryCodec[_]], decoder: ResolvingDecoder): Array[Any] = {
    val values = new Array[Any](readers.length)
    val fields = decoder.readFieldOrder()
    var i = 0
    while (i < fields.length) {
      val pos = fields(i).pos
      values(pos) = readers(pos).read(decoder)
      i += 1
    }
    values
  }
}
