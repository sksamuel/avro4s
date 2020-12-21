package com.sksamuel.avro4s

import com.sksamuel.avro4s.decoders.PrimitiveDecoders
import com.sksamuel.avro4s.encoders.{PrimitiveEncoders, StringEncoders}
import org.apache.avro.Schema

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
/**
 * An [[Decoder]] decodes a JVM value into a scala type T.
 *
 * For example, a Decoder[String] could convert an input of type Utf8 into a plain Java String.
 *
 * Another example, a decoder for Option[String] would handle inputs of null
 * by emitting a None, and a non-null input by emitting the decoded value
 * wrapped in a Some.
 */
trait Decoder[T] {
  self =>

  /**
   * Decodes the given AvroValue to an instance of T if possible, or
   * returns an AvroError.
   */
  def decode(value: Any): T

  /**
   * Creates a Decoder[U] by applying a function T => U to the
   * output of this decoder.
   */
  //  def map[U](f: T => U): Decoder[U] = new Decoder[U] :
  //    override def decode(value: Any) =
  //      self.decode(value) match {
  //        case error: AvroError => error
  //        case t: T => f(t)
  //      }
}

trait DecoderFor[T] { self =>
  
  def decoder(schema: Schema): Decoder[T]

  def map[U](f: T => U) = new DecoderFor[U] {
    override def decoder(schema: Schema): Decoder[U] = new Decoder[U] {
      override def decode(value: Any): U = f(self.decoder(schema).decode(value))
    }
  }
}

object DecoderFor extends PrimitiveDecoders {

  /**
   * Returns a [[DecoderFor]] that generates a value [T] without needing a schema.
   */
  def apply[T](f: Any => T) = new DecoderFor[T] {
    override def decoder(schema: Schema): Decoder[T] = new Decoder[T] {
      override def decode(value: Any): T = f(value)
    }
  }

  def apply[T](_decoder: Decoder[T]) = new DecoderFor[T] {
    override def decoder(schema: Schema): Decoder[T] = _decoder
  }
}

//trait StringDecoders {
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