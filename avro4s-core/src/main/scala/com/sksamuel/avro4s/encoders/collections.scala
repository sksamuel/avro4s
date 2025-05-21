package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.Encoder
import org.apache.avro.Schema

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait CollectionEncoders:

  private def iterableEncoder[T, C[X] <: Iterable[X]](encoder: Encoder[T]): Encoder[C[T]] = new Encoder[C[T]] {
    override def encode(schema: Schema): C[T] => AnyRef = {
      require(schema.getType == Schema.Type.ARRAY)
      val elementEncoder = encoder.encode(schema.getElementType)
      { t => t.map(elementEncoder.apply).toList.asJava }
    }
  }

  given[T](using encoder: Encoder[T], tag: ClassTag[T]): Encoder[Array[T]] = new Encoder[Array[T]] {
    override def encode(schema: Schema): Array[T] => AnyRef = {
      require(schema.getType == Schema.Type.ARRAY)
      val elementEncoder = encoder.encode(schema.getElementType)
      { t => t.map(elementEncoder.apply).toList.asJava }
    }
  }

  given[T](using encoder: Encoder[T]): Encoder[List[T]] = iterableEncoder(encoder)
  given[T](using encoder: Encoder[T]): Encoder[Seq[T]] = iterableEncoder(encoder)
  given[T](using encoder: Encoder[T]): Encoder[Set[T]] = iterableEncoder(encoder)
  given[T](using encoder: Encoder[T]): Encoder[Vector[T]] = iterableEncoder(encoder)

  given mapEncoder[T](using encoder: Encoder[T]): Encoder[Map[String, T]] = new MapEncoder[T](encoder)

class MapEncoder[T](encoder: Encoder[T]) extends Encoder[Map[String, T]] :
  override def encode(schema: Schema): Map[String, T] => AnyRef = {
    val encodeT = encoder.encode(schema.getValueType)
    { value =>
      val map = new java.util.HashMap[String, Any]
      value.foreach { case (k, v) => map.put(k, encodeT.apply(v)) }
      map
    }
  }
