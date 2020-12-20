//package com.sksamuel.avro4s
//
//import java.nio.ByteBuffer
//import java.util.UUID
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.{GenericData, GenericRecord}
//import org.apache.avro.specific.SpecificRecord
//import org.apache.avro.util.Utf8
//
//import scala.deriving.Mirror
//import scala.deriving._
//import scala.compiletime.{erasedValue, summonInline}
//
///**
// * An [[Encoder]] encodes a Scala value of type T into an [[AvroValue]]
// * based on the given schema.
// *
// * For example, an encoder could encode a String as an instance of Utf8,
// * or it could encode as an instance of GenericFixed.
// *
// * Another example is given a Scala enum value, the value could be encoded
// * as an instance of GenericData.EnumSymbol.
// *
// * Encoders can be created from a schema which controls how they encode
// * a particular type. See [[Encoder.create(schema)]].
// */
//trait Encoder[T] {
//  self =>
//
//  /**
//   * Encodes the given value T to an instance of AvroValue if possible,
//   * otherwise returns an AvroError.
//   */
//  def encode(value: T): AvroValue | AvroError
//
//  /**
//   * Returns an [[Encoder]] for type U by applying a function that maps a U
//   * to an T, before encoding as an T using this encoder.
//   */
//  def contramap[U](f: U => T): Encoder[U] = new Encoder[U] {
//    override def encode(value: U) = self.encode(f(value))
//  }
//
//  def map(f: AvroValue => AvroValue): Encoder[T] =
//    new Encoder[T] :
//      override def encode(value: T) = self.encode(value) match {
//        case error: AvroError => error
//        case value: AvroValue => f(value)
//      }
//}
//
//object Encoder extends PrimitiveEncoders with StringEncoders {
//
//  import scala.compiletime.{erasedValue, summonInline, constValue, constValueOpt}
//  import scala.jdk.CollectionConverters._
//
//  /**
//   * Creates a new [[Encoder]] from the given function f.
//   * The function is used to convert a scala value of type T to an avro value.
//   */
//  def apply[T](f: (T) => AvroValue): Encoder[T] = new Encoder[T] {
//    override def encode(value: T): AvroValue = f(value)
//  }
//}
//
//trait PrimitiveEncoders {
//
//  given Encoder[Byte] = Encoder(a => AvroValue.AvroByte(java.lang.Byte.valueOf(a)))
//  given Encoder[Short] = Encoder(a => AvroValue.AvroShort(java.lang.Short.valueOf(a)))
//  given Encoder[Int] = Encoder(a => AvroValue.AvroInt(java.lang.Integer.valueOf(a)))
//  given Encoder[Long] = Encoder(a => AvroValue.AvroLong(java.lang.Long.valueOf(a)))
//  given Encoder[Double] = Encoder(a => AvroValue.AvroDouble(java.lang.Double.valueOf(a)))
//  given Encoder[Float] = Encoder(a => AvroValue.AvroFloat(java.lang.Float.valueOf(a)))
//  given Encoder[Boolean] = Encoder(a => AvroValue.AvroBoolean(java.lang.Boolean.valueOf(a)))
//  given Encoder[ByteBuffer] = Encoder(a => AvroValue.AvroByteBuffer(a))
//}
//
//trait StringEncoders {
//
//  given Encoder[CharSequence] = Encoder(a => AvroValue.AvroString(a.toString))
//  given Encoder[UUID] = stringEncoder.contramap(x => x.toString)
//
//  given stringEncoder as Encoder[String] :
//    override def encode(value: String) = AvroValue.AvroUtf8(new Utf8(value))
////    private def encodeFixed(value: String): GenericData.Fixed = {
////      if (value.getBytes.length > schema.getFixedSize)
////        throw new Avro4sEncodingException(s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
////      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema).asInstanceOf[GenericData.Fixed]
////    }
//
////    override def encode(value: String): AvroValue = schema.getType match {
////      case Schema.Type.STRING => AvroValue.AvroUtf8(new Utf8(value))
////      case Schema.Type.FIXED => AvroValue.Fixed(encodeFixed(value, schema))
////      case Schema.Type.BYTES => AvroValue.AvroByteArray(value.getBytes)
////      case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
////    }
//
//  given Encoder[Utf8] :
//    override def encode(value: Utf8): AvroValue = AvroValue.AvroUtf8(value)
//}
//
///**
// * An implementation of org.apache.avro.generic.GenericContainer that is both a
// * GenericRecord and a SpecificRecord.
// */
//trait Record extends GenericRecord with SpecificRecord
//
//case class ImmutableRecord(schema: Schema, values: IndexedSeq[Any]) extends Record {
//
//  require(schema.getType == Schema.Type.RECORD, "Cannot create an ImmutableRecord with a schema that is not a RECORD")
//  require(schema.getFields.size == values.size,
//    s"Schema field size (${schema.getFields.size}) and value Seq size (${values.size}) must match")
//
//  override def put(key: String, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
//  override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
//
//  override def get(key: String): Any = {
//    val field = schema.getField(key)
//    if (field == null) sys.error(s"Field $key does not exist in this record (schema=$schema, values=$values)")
//    values(field.pos)
//  }
//
//  override def get(i: Int): Any = values(i)
//  override def getSchema: Schema = schema
//}