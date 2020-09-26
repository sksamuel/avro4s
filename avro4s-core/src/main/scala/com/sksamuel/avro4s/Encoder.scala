package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8

import scala.deriving.Mirror
import scala.deriving._
import scala.compiletime.{erasedValue, summonInline}

/**
 * An [[Encoder]] encodes a Scala value of type T into an [[AvroValue]]
 * based on the given schema.
 *
 * For example, encoding a string with a schema of type Schema.Type.STRING
 * would result in an instance of Utf8, whereas the same string and a
 * schema of type Schema.Type.FIXED would be encoded as an instance of GenericFixed.
 *
 * Another example is given a Scala enum value, and a schema of
 * type Schema.Type.ENUM, the value would be encoded as an instance
 * of GenericData.EnumSymbol.
 */
trait Encoder[T] {
  self =>

  /**
   * Encodes the given value T to an instance of AvroValue if possible,
   * otherwise returns an AvroError.
   */
  def encode(value: T, schema: Schema): AvroValue | AvroError

  /**
   * Returns an [[Encoder]] for type U by applying a function that maps a U
   * to an T, before encoding as an T using this encoder.
   */
  def contramap[U](f: U => T): Encoder[U] = new Encoder[U] {
    override def encode(value: U, schema: Schema) = self.encode(f(value), schema)
  }

  def map(f: AvroValue => AvroValue): Encoder[T] =
    new Encoder[T] :
      override def encode(value: T, schema: Schema) = self.encode(value, schema) match {
        case error: AvroError => error
        case value: AvroValue => f(value)
      }
}

object Encoder extends PrimitiveEncoders with StringEncoders {

  import scala.compiletime.{erasedValue, summonInline, constValue, constValueOpt}
  import scala.jdk.CollectionConverters._

  def apply[T](f: (T) => AvroValue): Encoder[T] = new Encoder[T] {
    override def encode(value: T, schema: Schema): AvroValue = f(value)
  }

  inline given derived[T](using m: Mirror.Of[T]) as Encoder[T] = {
    val encoders = summonAll[m.MirroredElemTypes]
    val labels = labelsToList[m.MirroredElemLabels]
    inline m match {
      case s: Mirror.SumOf[T] => ???
      case p: Mirror.ProductOf[T] => deriveProduct(p, encoders, labels)
    }
  }

  inline def labelsToList[T <: Tuple]: List[String] =
    inline erasedValue[T] match {
      case _: Unit => Nil
      case _: (head *: tail) => (inline constValue[head] match {
        case str: String => str
        case other => other.toString
      }) :: labelsToList[tail]
      // todo why is this Any required, why doesn't Unit grab the empty type?
      case _: Any => Nil
    }

  inline def summonAll[T]: List[Encoder[_]] = inline erasedValue[T] match {
    case _: EmptyTuple => Nil
    case _: (t *: ts) => summonInline[Encoder[t]] :: summonAll[ts]
  }

  inline def deriveProduct[T](p: Mirror.ProductOf[T], encoders: List[Encoder[_]], labels: List[String]): Encoder[T] = {
    new Encoder[T] {
      override def encode(value: T, schema: Schema): AvroValue.AvroRecord = {
        val map = value.asInstanceOf[Product].productIterator.zip(labels).zip(encoders).map { case ((value, label), encoder) =>
          val encoderAny = encoder.asInstanceOf[Encoder[Any]]
          val fieldSchema = schema.getField(label.toString).schema()
          val encodedValue = encoderAny.encode(value, fieldSchema) match {
            case error: AvroError => throw RuntimeException(s"error $error") // todo propogate as a term not by throwing
            case value: AvroValue => value.extract()
          }
          (label.toString, encodedValue)
        }.toMap

        // the record only includes fields declared in the schema
        // this allows us to build records that have a subset of the fields defined in a case class
        val values = schema.getFields.asScala.map { field =>
          map(field.name)
        }

        val record: GenericRecord = ImmutableRecord(schema, values.toIndexedSeq)

        new AvroValue.AvroRecord(record)
      }
    }
  }
}

trait PrimitiveEncoders {

  given Encoder[Byte] = Encoder(a => AvroValue.AvroByte(java.lang.Byte.valueOf(a)))
  given Encoder[Short] = Encoder(a => AvroValue.AvroShort(java.lang.Short.valueOf(a)))
  given Encoder[Int] = Encoder(a => AvroValue.AvroInt(java.lang.Integer.valueOf(a)))
  given Encoder[Long] = Encoder(a => AvroValue.AvroLong(java.lang.Long.valueOf(a)))
  given Encoder[Double] = Encoder(a => AvroValue.AvroDouble(java.lang.Double.valueOf(a)))
  given Encoder[Float] = Encoder(a => AvroValue.AvroFloat(java.lang.Float.valueOf(a)))
  given Encoder[Boolean] = Encoder(a => AvroValue.AvroBoolean(java.lang.Boolean.valueOf(a)))
  given Encoder[ByteBuffer] = Encoder(a => AvroValue.AvroByteBuffer(a))
}

trait StringEncoders {

  given Encoder[CharSequence] = Encoder(a => AvroValue.AvroString(a.toString))
  given Encoder[UUID] = stringEncoder.contramap(x => x.toString)

  given stringEncoder as Encoder[String] :
    private def encodeFixed(value: String, schema: Schema): GenericData.Fixed = {
      if (value.getBytes.length > schema.getFixedSize)
        throw new Avro4sEncodingException(s"Cannot write string with ${value.getBytes.length} bytes to fixed type of size ${schema.getFixedSize}")
      GenericData.get.createFixed(null, ByteBuffer.allocate(schema.getFixedSize).put(value.getBytes).array, schema).asInstanceOf[GenericData.Fixed]
    }

    override def encode(value: String, schema: Schema): AvroValue = schema.getType match {
      case Schema.Type.STRING => AvroValue.AvroUtf8(new Utf8(value))
      case Schema.Type.FIXED => AvroValue.Fixed(encodeFixed(value, schema))
      case Schema.Type.BYTES => AvroValue.AvroByteArray(value.getBytes)
      case _ => throw new Avro4sConfigurationException(s"Unsupported type for string schema: $schema")
    }

  given Encoder[Utf8] :
    override def encode(value: Utf8, schema: Schema): AvroValue = AvroValue.AvroUtf8(value)
}

/**
 * An implementation of org.apache.avro.generic.GenericContainer that is both a
 * GenericRecord and a SpecificRecord.
 */
trait Record extends GenericRecord with SpecificRecord

case class ImmutableRecord(schema: Schema, values: IndexedSeq[Any]) extends Record {

  require(schema.getType == Schema.Type.RECORD, "Cannot create an ImmutableRecord with a schema that is not a RECORD")
  require(schema.getFields.size == values.size,
    s"Schema field size (${schema.getFields.size}) and value Seq size (${values.size}) must match")

  override def put(key: String, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")
  override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException("This implementation of Record is immutable")

  override def get(key: String): Any = {
    val field = schema.getField(key)
    if (field == null) sys.error(s"Field $key does not exist in this record (schema=$schema, values=$values)")
    values(field.pos)
  }

  override def get(i: Int): Any = values(i)
  override def getSchema: Schema = schema
}