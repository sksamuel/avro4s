package com.sksamuel.avro4s.encoder

import com.sksamuel.avro4s.AvroValue
import com.sksamuel.avro4s.{AvroSchema, Encoder, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.junit.Test

class BasicEncoderTest {

  @Test def `encode strings as UTF8`() = {
    case class Foo(s: String)
    val schema = AvroSchema[Foo]
    val encoder = Encoder.derived[Foo]
    val value = encoder.encode(Foo("hello"), schema)
    val expected = ImmutableRecord(schema, IndexedSeq(new Utf8("hello")))
    value match {
      case AvroValue.AvroRecord(record) =>
        assert(record == expected)
    }
  }

  @Test def `encode doubles`() = {
    case class Foo(d: Double)
    val schema = AvroSchema[Foo]
    val encoder = Encoder.derived[Foo]
    val value = encoder.encode(Foo(123.45), schema)
    val expected = ImmutableRecord(schema, IndexedSeq(123.45))
    value match {
      case AvroValue.AvroRecord(record) =>
        assert(record == expected)
    }
  }
}
