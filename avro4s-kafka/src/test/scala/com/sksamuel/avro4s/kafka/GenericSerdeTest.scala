package com.sksamuel.avro4s.kafka

import com.sksamuel.avro4s.{BinaryFormat, DataFormat, JsonFormat}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class TheKafkaValue(name: String, location: String)

class GenericSerdeTest extends AnyFlatSpec with Matchers {

  val someValue = TheKafkaValue("the name", "the location")

  "serialization with default binary avro format" should " be identical to binary format" in  {
    val defaultSerde = new GenericSerde[TheKafkaValue]()
    val binarySerde = new GenericSerde[TheKafkaValue](BinaryFormat)

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
    val binarySerde = new GenericSerde[TheKafkaValue](BinaryFormat)

    binarySerde.deserialize(
      "any-topic",
      binarySerde.serialize("any-topic", someValue)
    ) shouldBe someValue
  }

  "round trip with json avro format" should "yield back original data" in  {
    val jsonSerde = new GenericSerde[TheKafkaValue](JsonFormat)

    jsonSerde.deserialize(
      "any-topic",
      jsonSerde.serialize("any-topic", someValue)
    ) shouldBe someValue
  }

  "round trip with data avro format" should "yield back original data" in  {
    val dataSerde = new GenericSerde[TheKafkaValue](DataFormat)

    dataSerde.deserialize(
      "any-topic",
      dataSerde.serialize("any-topic", someValue)
    ) shouldBe someValue
  }

}
