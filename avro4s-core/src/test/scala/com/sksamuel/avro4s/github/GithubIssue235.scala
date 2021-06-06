//package com.sksamuel.avro4s.github
//
//import java.io.ByteArrayOutputStream
//
//import com.sksamuel.avro4s.{Decoder, Encoder, RecordFormat, SchemaFor}
//import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
//import org.apache.avro.io.{DecoderFactory, EncoderFactory}
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//case class Label(value: String) extends AnyVal
//case class Value[A](label: Label, value: A)
//
//sealed trait OneOrTwo[A]
//case class One[A](value: Value[A]) extends OneOrTwo[A]
//case class Two[A](first: Value[A], second: Value[A]) extends OneOrTwo[A]
//case class OneOrTwoWrapper[A](t: OneOrTwo[A])
//
//object Bug {
//
//  def apply[T <: Product](a: T)(
//    implicit schemaFor: SchemaFor[T],
//    encoder: Encoder[T],
//    decoder: Decoder[T]
//  ): Unit = {
//
//    val format = RecordFormat[T]
//    val schema = schemaFor.schema
//    val datumReader = new GenericDatumReader[GenericRecord](schema)
//    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
//
//    val stream = new ByteArrayOutputStream()
//    val bEncoder = EncoderFactory.get().binaryEncoder(stream, null)
//
//    datumWriter.write(format.to(a), bEncoder)
//    bEncoder.flush()
//
//    val bytes = stream.toByteArray
//    val bDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
//    val record = datumReader.read(null, bDecoder)
//    require(format.from(record) == a)
//  }
//
//}
//
//class GithubIssue235 extends AnyFunSuite with Matchers {
//  test("Broken typeclass derivation upgrading from 1.9.0 to 2.0.1 #235") {
//    val o = OneOrTwoWrapper(One(Value(Label("lbl"), "foo")))
//    Bug(o)
//  }
//}