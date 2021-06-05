package com.sksamuel.avro4s.encoders

import com.sksamuel.avro4s.Encoder
import org.apache.avro.Schema

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait CollectionEncoders:

  private def iterableEncoder[T, C[X] <: Iterable[X]](encoder: Encoder[T]): Encoder[C[T]] = new Encoder[C[T]] {
    override def encode(schema: Schema): C[T] => Any = {
      val elementEncoder = encoder.encode(schema)
      { t => t.map(elementEncoder.apply).toList.asJava }
    }
  }

  given[T](using encoder: Encoder[T], tag: ClassTag[T]): Encoder[Array[T]] = new Encoder[Array[T]] {
    override def encode(schema: Schema): Array[T] => Any = {
      val elementEncoder = encoder.encode(schema)
      { t => t.map(elementEncoder.apply).toList.asJava }
    }
  }

  given[T](using encoder: Encoder[T]): Encoder[List[T]] = iterableEncoder(encoder)
  given[T](using encoder: Encoder[T]): Encoder[Seq[T]] = iterableEncoder(encoder)
  given[T](using encoder: Encoder[T]): Encoder[Set[T]] = iterableEncoder(encoder)
  given[T](using encoder: Encoder[T]): Encoder[Vector[T]] = iterableEncoder(encoder)

//  implicit def mapEncoder[T](implicit value: Encoder[T]): Encoder[Map[String, T]] =
//    new ResolvableEncoder[Map[String, T]] {
//      def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[Map[String, T]] = {
//        val encoder = value.resolveEncoder(env, mapFullUpdate(extractMapValueSchema, update))
//
//        new Encoder[Map[String, T]] {
//          val schemaFor: SchemaFor[Map[String, T]] = buildMapSchemaFor(encoder.schemaFor)
//
//          def encode(value: Map[String, T]): AnyRef = {
//            val map = new util.HashMap[String, AnyRef]
//            value.foreach { case (k, v) => map.put(k, encoder.encode(v)) }
//            map
//          }
//
//          override def withSchema(schemaFor: SchemaFor[Map[String, T]]): Encoder[Map[String, T]] =
//            buildWithSchema(mapEncoder(value), schemaFor)
//        }
//      }
//    }