package com.sksamuel.avro4s.kafka

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroFormat, AvroInputStream, AvroOutputStream, BinaryFormat, DataFormat, Decoder, Encoder, JsonFormat, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

/**
  * Kafka Serde using Avro4s for serializing to/deserialising from case classes into Avro records, without integration
  * with the Confluent schema registry.
 *
 * The implicit schemaFor instance is used as the writer schema when deserializing, in case it needs to diverge
 * from both writer schema used in serialize, and the desired schema in deserialize.
 */
class GenericSerde[T >: Null : SchemaFor : Encoder : Decoder](avroFormat: AvroFormat = BinaryFormat) extends Serde[T]
  with Deserializer[T]
  with Serializer[T]
  with Serializable {

  val schema: Schema = SchemaFor[T].schema

  override def serializer(): Serializer[T] = this

  override def deserializer(): Deserializer[T] = this

  override def deserialize(topic: String, data: Array[Byte]): T = {
    if (data == null) null else {

      val avroInputStream = avroFormat match {
        case BinaryFormat => AvroInputStream.binary[T]
        case JsonFormat => AvroInputStream.json[T]
        case DataFormat => AvroInputStream.data[T]
      }

      val input = avroInputStream.from(data).build(schema)
      val result = input.iterator.next()
      input.close()
      result
    }
  }

  override def close(): Unit = ()

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()

    val avroOutputStream = avroFormat match {
      case BinaryFormat => AvroOutputStream.binary[T]
      case JsonFormat => AvroOutputStream.json[T]
      case DataFormat => AvroOutputStream.data[T]
    }

    val output = avroOutputStream.to(baos).build()
    output.write(data)
    output.close()
    baos.toByteArray
  }
}