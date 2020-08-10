package com.sksamuel.avro4s

class Avro4sException(message: String) extends Exception(message: String)

class Avro4sConfigurationException(message: String) extends Avro4sException(message)

class Avro4sEncodingException(message: String, val value: Any, val encoder: Encoder[_]) extends Avro4sException(message) {
  def schema = encoder.schema
}

class Avro4sDecodingException(message: String, val value: Any, val decoder: Decoder[_]) extends Avro4sException(message) {
  def schema = decoder.schema
}

class Avro4sUnsupportedValueException(message: String) extends Avro4sException(message)

object Avro4sUnsupportedValueException {
  def apply(value: AvroValue, decoder: Decoder[_]) =
    new Avro4sDecodingException(s"Unsupported type for this decoder [value=$value, decoder=$decoder]", value.toString, decoder)
}