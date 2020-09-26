package com.sksamuel.avro4s.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder, AvroValue}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.junit.Test

class BasicDecoderTest {

  @Test def `decode strings from UTF8`() = {
    case class Foo(str: String)
    val schema = AvroSchema[Foo]
    val record = new GenericData.Record(schema)
    record.put("str", new Utf8("hello"))
    val decoder = Decoder.derived[Foo]
    val actual = decoder.decode(AvroValue.AvroRecord(record), schema)
    val expected = Foo("hello")
    assert(actual == expected)
  }

  @Test def `decode doubles from avro double`() = {
    case class Foo(d: Double)
    val schema = AvroSchema[Foo]
    val record = new GenericData.Record(schema)
    record.put("d", 123.123)
    val decoder = Decoder.derived[Foo]
    val actual = decoder.decode(AvroValue.AvroRecord(record), schema)
    val expected = Foo(123.123)
    assert(actual == expected)
  }
}
