package benchmarks

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.Collections

import benchmarks.record._
import com.sksamuel.avro4s._
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.ByteBufferInputStream
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object Decoding extends BenchmarkHelpers {
  trait Setup {
    val (handrolledDecoder, handrolledReader) = {
      import benchmarks.handrolled_codecs._
      implicit val codec: Codec[AttributeValue[Int]] = AttributeValueCodec[Int]
      implicit val schemaFor: SchemaFor[AttributeValue[Int]] = SchemaFor[AttributeValue[Int]](codec.schema)
      val recordSchemaFor = SchemaFor[RecordWithUnionAndTypeField]
      val decoder = Decoder[RecordWithUnionAndTypeField].withSchema(recordSchemaFor)
      val reader = new GenericDatumReader[GenericRecord](recordSchemaFor.schema)
      (decoder, reader)
    }

    val (avro4sDecoder, avro4sReader) = {
      val decoder = Decoder[RecordWithUnionAndTypeField]
      val reader = new GenericDatumReader[GenericRecord](decoder.schema)
      (decoder, reader)
    }
  }

  @State(Scope.Thread)
  class AvroBytesSetup extends Setup {
    val avroBytes = {
      import benchmarks.record.generated.AttributeValue._
      import benchmarks.record.generated._
      new RecordWithUnionAndTypeField(new ValidInt(255, t)).toByteBuffer
    }
  }

  @State(Scope.Thread)
  class Avro4sBytesSetup extends Setup {
    val avro4sBytes = encode(RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t)))
  }

  @State(Scope.Thread)
  class AvroInputStreamSetup extends Setup {
    private val outputStream = new ByteArrayOutputStream()
    private val dataOutputStream = new AvroDataOutputStream[RecordWithUnionAndTypeField](outputStream, CodecFactory.nullCodec())
    private val obj = RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t))
    dataOutputStream.write(0.to(255).map(i => RecordWithUnionAndTypeField(AttributeValue.Valid[Int](i, t))))
    dataOutputStream.close()
    private val bytes = outputStream.toByteArray

    def avroDataStream =
      new AvroDataInputStream[RecordWithUnionAndTypeField](new ByteArrayInputStream(bytes), None)
  }

  def encode[T: Encoder: SchemaFor](value: T): ByteBuffer = {
    val outputStream = new ByteArrayOutputStream(512)
    val encoder = Encoder[T]
    val schema = AvroSchema[T]
    val record = encoder.encode(value).asInstanceOf[GenericRecord]
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val enc = EncoderFactory.get().directBinaryEncoder(outputStream, null)
    writer.write(record, enc)
    ByteBuffer.wrap(outputStream.toByteArray)
  }
}

class Decoding extends CommonParams with BenchmarkHelpers {

  import Decoding._

  def decode[T](bytes: ByteBuffer, decoder: Decoder[T], reader: GenericDatumReader[GenericRecord]): T = {
    val dec =
      DecoderFactory.get().binaryDecoder(new ByteBufferInputStream(Collections.singletonList(bytes.duplicate)), null)
    val record = reader.read(null, dec)
    decoder.decode(record)
  }


  @Benchmark
  def avroSpecificRecord(setup: AvroBytesSetup, blackhole: Blackhole) = {
    import benchmarks.record.generated._
    blackhole.consume(RecordWithUnionAndTypeField.fromByteBuffer(setup.avroBytes.duplicate))
  }

  @Benchmark
  def avro4sHandrolled(setup: Avro4sBytesSetup, blackhole: Blackhole) =
    blackhole.consume(decode(setup.avro4sBytes, setup.handrolledDecoder, setup.handrolledReader))

  @Benchmark
  def avro4sGenerated(setup: Avro4sBytesSetup, blackhole: Blackhole) =
    blackhole.consume(decode(setup.avro4sBytes, setup.avro4sDecoder, setup.avro4sReader))
  @Benchmark
  def avroDataInputStream(setup: AvroInputStreamSetup, blackhole: Blackhole) = {
    blackhole.consume(setup.avroDataStream.iterator.toList)
  }
}
