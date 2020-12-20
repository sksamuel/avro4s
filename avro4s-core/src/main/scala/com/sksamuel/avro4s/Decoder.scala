//package com.sksamuel.avro4s
//
//import java.nio.ByteBuffer
//import java.util.UUID
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.util.Utf8
//
//import scala.deriving.Mirror
//
///**
// * An [[Decoder]] decodes an [[AvroValue]] into a scala type T
// * using the given schema.
// *
// * For example, a Decoder[String] would convert an input of type Utf8 -
// * which is one of the ways Avro can encode strings - into a plain Java String.
// *
// * Another example, a decoder for Option[String] would handle inputs of null
// * by emitting a None, and a non-null input by emitting the decoded value
// * wrapped in a Some.
// */
//trait Decoder[T] {
//  self =>
//
//  /**
//   * Decodes the given AvroValue to an instance of T if possible, or
//   * returns an AvroError.
//   */
//  def decode(value: AvroValue, schema: Schema): T | AvroError
//
//  /**
//   * Creates a Decoder[U] by applying a function T => U to the
//   * output of this decoder.
//   */
//  def map[U](f: T => U): Decoder[U] =
//    new Decoder[U] :
//      override def decode(value: AvroValue, schema: Schema) =
//        self.decode(value, schema) match {
//          case error: AvroError => error
//          case t: T => f(t)
//        }
//}
//
//enum AvroError {
//  case DecodingError(msg: String)
//}
//
//trait PrimitiveDecoders {
//
//  given Decoder[ByteBuffer] with {
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroByteBuffer(bb) => bb
//      case AvroValue.AvroByteArray(bytes) => ByteBuffer.wrap(bytes)
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => ByteBuffer")
//    }
//  }
//
//  given Decoder[Boolean] :
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroBoolean(a) => a
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => Boolean")
//    }
//
//
//  given Decoder[Int] :
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroByte(a) => a.toInt
//      case AvroValue.AvroShort(a) => a.toInt
//      case AvroValue.AvroInt(a) => a.toInt
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => Int")
//    }
//
//  given Decoder[Long] :
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroLong(a) => a
//      case AvroValue.AvroByte(a) => a.toLong
//      case AvroValue.AvroShort(a) => a.toLong
//      case AvroValue.AvroInt(a) => a.toLong
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => Long")
//    }
//
//  given Decoder[Double] :
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroDouble(d) => d
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => Double")
//    }
//
//  given Decoder[Float] :
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroFloat(f) => f
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => Float")
//    }
//}
//
//trait StringDecoders {
//
//  given stringDecoder as Decoder[String] :
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroString(str) => str
//      case AvroValue.AvroUtf8(utf8) => utf8.toString()
//      case AvroValue.Fixed(fixed) => new String(fixed.bytes())
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => String")
//    }
//
//  given Decoder[Utf8] :
//    override def decode(value: AvroValue, schema: Schema) = value match {
//      case AvroValue.AvroUtf8(utf8) => utf8
//      case AvroValue.AvroString(str) => new Utf8(str)
//      case AvroValue.AvroByteArray(bytes) => new Utf8(bytes)
//      case AvroValue.AvroByteBuffer(bb) => new Utf8(bb.array)
//      case AvroValue.Fixed(fixed) => new Utf8(fixed.bytes())
//      case _ => AvroError.DecodingError(s"Unsupported operation $value => Utf8")
//    }
//
//  given Decoder[UUID] = stringDecoder.map(UUID.fromString)
//}
//
//object Decoder extends PrimitiveDecoders with StringDecoders {
//
//  import scala.compiletime.{erasedValue, summonInline, constValue, constValueOpt}
//  import scala.jdk.CollectionConverters._
//
//  inline given derived[T](using m: Mirror.Of[T]) as Decoder[T] = {
//
//    val decoders = summonAll[m.MirroredElemTypes]
//    val labels = labelsToList[m.MirroredElemLabels]
//
//    inline m match {
//      case s: Mirror.SumOf[T] => ???
//      case p: Mirror.ProductOf[T] => deriveProduct(p, decoders, labels)
//    }
//  }
//
//  inline def summonAll[T]: List[Decoder[_]] = inline erasedValue[T] match {
//    case _: EmptyTuple => Nil
//    case _: (t *: ts) => summonInline[Decoder[t]] :: summonAll[ts]
//  }
//
//  inline def labelsToList[T <: Tuple]: List[String] =
//    inline erasedValue[T] match {
//      case _: Unit => Nil
//      case _: (head *: tail) => (inline constValue[head] match {
//        case str: String => str
//        case other => other.toString
//      }) :: labelsToList[tail]
//      // todo why is this Any required, why doesn't Unit grab the empty type?
//      case _: Any => Nil
//    }
//
//  inline def deriveProduct[T](p: Mirror.ProductOf[T], decoders: List[Decoder[_]], labels: List[String]): Decoder[T] = {
//    new Decoder[T] {
//      override def decode(value: AvroValue, schema: Schema) = {
//        val record: GenericRecord = value.asInstanceOf[AvroValue.AvroRecord].record
//        val values = labels.zip(decoders).map { case (label, decoder) =>
//          val avroValue = unsafeFromAny(record.get(label))
//          decoder.decode(avroValue, schema.getField(label).schema())
//        }
//        p.fromProduct(Tuple.fromArray(values.toArray))
//      }
//    }
//  }
//}