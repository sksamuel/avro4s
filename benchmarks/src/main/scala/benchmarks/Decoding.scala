package benchmarks

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.Collections
import benchmarks.record._
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.ByteBufferInputStream
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object Decoding extends BenchmarkHelpers {
  @State(Scope.Thread)
  class Setup {
    val avroBytes = {
      import benchmarks.record.generated.AttributeValue._
      import benchmarks.record.generated._
      new RecordWithUnionAndTypeField(new ValidInt(255, t)).toByteBuffer
    }

    val avro4sBytes = encode(RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t)))

    //    val (handrolledDecoder, handrolledReader) = {
    //      import benchmarks.handrolled_codecs._
    //      implicit val codec: Codec[AttributeValue[Int]] = AttributeValueCodec[Int]
    //      implicit val schemaFor: SchemaFor[AttributeValue[Int]] = SchemaFor[AttributeValue[Int]](codec.schema)
    //      val recordSchemaFor = SchemaFor[RecordWithUnionAndTypeField]
    //      val decoder = Decoder[RecordWithUnionAndTypeField].withSchema(recordSchemaFor)
    //      val reader = new GenericDatumReader[GenericRecord](recordSchemaFor.schema)
    //      (decoder, reader)
    //    }

    val (schema, avro4sDecoder, avro4sReader) = {
      val schema = AvroSchema[RecordWithUnionAndTypeField]
      val decoder = Decoder[RecordWithUnionAndTypeField].decode(schema)
      val reader = new GenericDatumReader[GenericRecord](schema)
      (schema, decoder, reader)
    }
  }

  def encode[T: Encoder: SchemaFor](value: T): ByteBuffer = {
    val outputStream = new ByteArrayOutputStream(512)
    val encoder = Encoder[T]
    val schema = AvroSchema[T]
    val record = encoder.encode(schema).apply(value).asInstanceOf[GenericRecord]
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val enc = EncoderFactory.get().directBinaryEncoder(outputStream, null)
    writer.write(record, enc)
    ByteBuffer.wrap(outputStream.toByteArray)
  }
}

class Decoding extends CommonParams with BenchmarkHelpers {

  import Decoding._

  def decode[T](bytes: ByteBuffer, schema: Schema, decoder: Any => T, reader: GenericDatumReader[GenericRecord]): T = {
    val dec =
      DecoderFactory.get().binaryDecoder(new ByteBufferInputStream(Collections.singletonList(bytes.duplicate)), null)
    val record = reader.read(null, dec)
    decoder.apply(record)
  }


  @Benchmark
  def avroSpecificRecord(setup: Setup, blackhole: Blackhole) = {
    import benchmarks.record.generated._
    blackhole.consume(RecordWithUnionAndTypeField.fromByteBuffer(setup.avroBytes.duplicate))
  }

  //  @Benchmark
  //  def avro4sHandrolled(setup: Setup, blackhole: Blackhole) =
  //    blackhole.consume(decode(setup.avro4sBytes, setup.handrolledDecoder, setup.handrolledReader))
  //
  @Benchmark
  def avro4sGenerated(setup: Setup, blackhole: Blackhole) =
    blackhole.consume(decode(setup.avro4sBytes, setup.schema, setup.avro4sDecoder, setup.avro4sReader))
}
