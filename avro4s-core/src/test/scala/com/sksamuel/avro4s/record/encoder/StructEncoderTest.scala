package com.sksamuel.avro4s.record.encoder

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.TimeMeasurementUtils._
import com.sksamuel.avro4s.{AvroSchema, DefaultFieldMapper, Encoder, ImmutableRecord, Record}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

case class County(name: String, towns: Seq[Town], ceremonial: Boolean, lat: Double, long: Double)
case class Town(name: String, population: Int)

case class ImmutableRecordArray(schema: Schema, values: Array[AnyRef]) extends Record {
  require(schema.getType == Schema.Type.RECORD, "Cannot create an ImmutableRecord with a schema that is not a RECORD")

  import scala.collection.JavaConverters._

  override def put(key: String, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
  override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")

  override def get(key: String): AnyRef = {
    val index = schema.getFields.asScala.indexWhere(_.name == key)
    if (index == -1)
      sys.error(s"Field $key does not exist in this record (schema=$schema, values=$values)")
    get(index)
  }

  override def get(i: Int): AnyRef = values(i)
  override def getSchema: Schema = schema
}

class StructEncoderTest extends WordSpec with Matchers {

  import scala.collection.JavaConverters._

  private val encoderFactory = EncoderFactory.get()

  private def emulateEncodingForSpecificRecord(obj: SpecificRecord): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val writer = new SpecificDatumWriter[AnyRef](obj.getSchema)
    val encoder = encoderFactory.directBinaryEncoder(out, null)
    writer.write(obj, encoder)
    encoder.flush()
    val result = out.toByteArray
    out.close()
    result
  }

  private def emulateEncodingForGenericRecord(obj: GenericRecord): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val writer = new GenericDatumWriter[AnyRef](obj.getSchema)
    val encoder = encoderFactory.directBinaryEncoder(out, null)
    writer.write(obj, encoder)
    encoder.flush()
    val result = out.toByteArray
    out.close()
    result
  }

  "RecordEncoder" should {
    "encode structs" in {
      val countySchema = AvroSchema[County]
      val townSchema = AvroSchema[Town]
      val count = County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)
      val result = Encoder[County].encode(count, countySchema, DefaultFieldMapper)

      val hardwick = ImmutableRecord(townSchema, Vector(new Utf8("Hardwick"), java.lang.Integer.valueOf(123)))
      val weedon = ImmutableRecord(townSchema, Vector(new Utf8("Weedon"), java.lang.Integer.valueOf(225)))
      result shouldBe ImmutableRecord(countySchema, Vector(new Utf8("Bucks"), List(hardwick, weedon).asJava, java.lang.Boolean.valueOf(true), java.lang.Double.valueOf(12.34), java.lang.Double.valueOf(0.123)))
    }
    "be fast enough" in {
      val countySchema = AvroSchema[County]
      val county = County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)

      val immutableRecord = Encoder[County].encode(county, countySchema, DefaultFieldMapper).asInstanceOf[ImmutableRecord]
      val immutableRecordArray = ImmutableRecordArray(immutableRecord.schema, immutableRecord.values.toArray)

      val n = 1000000

      //warming up
      (0 until n) foreach { _ => emulateEncodingForGenericRecord(immutableRecordArray) }
      (0 until n) foreach { _ => emulateEncodingForSpecificRecord(immutableRecordArray) }
      (0 until n) foreach { _ => emulateEncodingForGenericRecord(immutableRecord) }
      (0 until n) foreach { _ => emulateEncodingForSpecificRecord(immutableRecord) }

      logTime("ImmutableRecordArray. Generic serialization") {
        (0 until n) foreach { _ => emulateEncodingForGenericRecord(immutableRecordArray) }
      }
      logTime("ImmutableRecordArray. Specific serialization") {
        (0 until n) foreach { _ => emulateEncodingForSpecificRecord(immutableRecordArray) }
      }

      logTime("ImmutableRecord. Generic serialization") {
        (0 until n) foreach { _ => emulateEncodingForGenericRecord(immutableRecord) }
      }
      logTime("ImmutableRecord. Specific serialization") {
        (0 until n) foreach { _ => emulateEncodingForSpecificRecord(immutableRecord) }
      }
    }
  }
}
