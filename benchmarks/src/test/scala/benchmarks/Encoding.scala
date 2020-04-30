package benchmarks

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import benchmarks.record._
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.scalameter.Context
import org.scalameter.api._

object Encoding extends Bench.LocalTime with BenchmarkHelpers {
  override def defaultConfig: Context = Context(exec.minWarmupRuns -> 10000, exec.benchRuns -> 10000)

  def encode[T](value: T,
                encoder: Encoder[T],
                writer: GenericDatumWriter[GenericRecord],
                schema: Schema): ByteBuffer = {
    val outputStream = new ByteArrayOutputStream(512)
    val record = encoder.encode(value).asInstanceOf[GenericRecord]
    val enc = EncoderFactory.get().directBinaryEncoder(outputStream, null)
    writer.write(record, enc)
    ByteBuffer.wrap(outputStream.toByteArray)
  }

  performance of "avro4s simple field encoding" in {

    val schema = AvroSchema[RecordWithSimpleField]
    val encoder = Encoder[RecordWithSimpleField]
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val s = RecordWithSimpleField(IntAttributeValue.Valid(255, t))

    using(item) in { _ =>
      encode(s, encoder, writer, schema)
    }
  }

  performance of "avro4s type union encoding" in {

    val schema = AvroSchema[RecordWithUnionField]
    val encoder = Encoder[RecordWithUnionField]
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val s = RecordWithUnionField(IntAttributeValue.Valid(255, t))

    using(item) in { _ =>
      encode(s, encoder, writer, schema)
    }
  }

  performance of "avro4s type parameter encoding" in {

    val schema = AvroSchema[RecordWithTypeParamField]
    val encoder = Encoder[RecordWithTypeParamField]
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val s = RecordWithTypeParamField(AttributeValue.Valid[Int](255, t))

    using(item) in { _ =>
      encode(s, encoder, writer, schema)
    }
  }

  performance of "avro4s union type with type param encoding" in {

    val schema = AvroSchema[RecordWithUnionAndTypeField]
    val encoder = Encoder[RecordWithUnionAndTypeField]
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val s = RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t))

    using(item) in { _ =>
      encode(s, encoder, writer, schema)
    }
  }

  performance of "Avro specific record union type field encoding" in {

    import benchmarks.record.generated.AttributeValue._
    import benchmarks.record.generated._
    val s = new RecordWithUnionAndTypeField(new ValidInt(255, t))

    using(item) in { _ =>
      s.toByteBuffer
    }
  }

  performance of "avro4s union type with type param hand-rolled encoding" in {

    import benchmarks.handrolled_codecs._
    val codec: AttributeValueCodec[Int] = AttributeValueCodec[Int]
    implicit val attributeValueEncoder = codec.encoder
    implicit val attributeValueDecoder = codec.decoder
    implicit val schemaForValid = codec.schemaForValid
    val schema = AvroSchema[RecordWithUnionAndTypeField]
    val encoder = Encoder[RecordWithUnionAndTypeField]
    val writer = new GenericDatumWriter[GenericRecord](schema)

    val s = RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t))

    using(item) in { _ =>
      encode(s, encoder, writer, schema)
    }
  }

  performance of "avro4s union type with type param" in {
    val encoder = Encoder[RecordWithUnionAndTypeField]
    val writer = new GenericDatumWriter[GenericRecord](encoder.schema)

    val s = RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t))

    using(item) in { _ =>
      val outputStream = new ByteArrayOutputStream(512)
      val record = encoder.encode(s).asInstanceOf[GenericRecord]
      val enc = EncoderFactory.get().directBinaryEncoder(outputStream, null)
      writer.write(record, enc)
      ByteBuffer.wrap(outputStream.toByteArray)
    }
  }
}
