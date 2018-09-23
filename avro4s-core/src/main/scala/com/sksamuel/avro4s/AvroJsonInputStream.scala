//package com.sksamuel.avro4s
//
//import java.io.InputStream
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.io.DecoderFactory
//
//import scala.util.Try
//
//final case class AvroJsonInputStream[T](in: InputStream, writerSchema: Option[Schema] = None, readerSchema: Option[Schema] = None)
//                                       (implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
//  extends AvroInputStream[T] {
//
//  val wSchema = writerSchema.getOrElse(schemaFor())
//  val rSchema = readerSchema.getOrElse(schemaFor())
//  private val datumReader = new DefaultAwareDatumReader[GenericRecord](wSchema, rSchema, new DefaultAwareGenericData)
//  private val jsonDecoder = DecoderFactory.get.jsonDecoder(wSchema, in)
//
//  private def next = Try {
//    datumReader.read(null, jsonDecoder)
//  }
//
//  def iterator: Iterator[T] = Iterator.continually(next)
//    .takeWhile(_.isSuccess)
//    .map(_.get)
//    .map(fromRecord.apply)
//
//  def tryIterator: Iterator[Try[T]] = Iterator.continually(next)
//    .takeWhile(_.isSuccess)
//    .map(_.get)
//    .map(record => Try(fromRecord(record)))
//
//  def singleEntity: Try[T] = next.map(fromRecord.apply)
//
//  override def close(): Unit = in.close()
//}
