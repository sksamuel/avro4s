package com.sksamuel.avro4s.kafka

import com.sksamuel.avro4s.AvroFormat
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class TheKafkaValue(name: String, location: String)

class GenericSerdeTest extends AnyFlatSpec with Matchers {

  val someValue = TheKafkaValue("the name", "the location")

  "serialization with default binary avro format" should " be identical to binary format" in  {
    val defaultSerde = new GenericSerde[TheKafkaValue]()
    val binarySerde = new GenericSerde[TheKafkaValue](AvroFormat.Binary)

    defaultSerde.serialize("any-topic", someValue) shouldBe binarySerde.serialize("any-topic", someValue)
  }

  "round trip with default (binary) avro format" should "yield back original data" in  {
    val defaultSerde = new GenericSerde[TheKafkaValue]()

    defaultSerde.deserialize(
      "any-topic",
      defaultSerde.serialize("any-topic", someValue)
    ) shouldBe someValue
  }

  "round trip with binary avro format" should "yield back original data" in  {
    val binarySerde = new GenericSerde[TheKafkaValue](AvroFormat.Binary)

    binarySerde.deserialize(
      "any-topic",
      binarySerde.serialize("any-topic", someValue)
    ) shouldBe someValue
  }

  "round trip with json avro format" should "yield back original data" in  {
    val jsonSerde = new GenericSerde[TheKafkaValue](AvroFormat.Json)

    jsonSerde.deserialize(
      "any-topic",
      jsonSerde.serialize("any-topic", someValue)
    ) shouldBe someValue
  }

  "round trip with data avro format" should "yield back original data" in  {
    val dataSerde = new GenericSerde[TheKafkaValue](AvroFormat.Data)

    dataSerde.deserialize(
      "any-topic",
      dataSerde.serialize("any-topic", someValue)
    ) shouldBe someValue
  }

  "round trip with tombstone" should "yield back tombstone" in {
    val binarySerde = new GenericSerde[TheKafkaValue](AvroFormat.Binary)
    val tombstone: TheKafkaValue = null
    binarySerde.deserialize("any-topic", binarySerde.serialize("any-topic", tombstone)) shouldBe tombstone
  }
}
