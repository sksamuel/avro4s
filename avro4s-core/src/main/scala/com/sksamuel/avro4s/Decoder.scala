package com.sksamuel.avro4s

trait Decoder[T] {
  def decode(value: AvroValue): T
}
