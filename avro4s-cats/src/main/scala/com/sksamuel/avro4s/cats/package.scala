package com.sksamuel.avro4s

import _root_.cats.data.NonEmptyList
import _root_.cats.data.NonEmptyVector
import _root_.cats.data.NonEmptyChain
import org.apache.avro.Schema

import scala.language.implicitConversions

package object cats:

  import scala.collection.JavaConverters._

  given[T](using schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] = SchemaFor(Schema.createArray(schemaFor.schema))
  given[T](using schemaFor: SchemaFor[T]): SchemaFor[NonEmptyVector[T]] = SchemaFor(Schema.createArray(schemaFor.schema))
  given[T](using schemaFor: SchemaFor[T]): SchemaFor[NonEmptyChain[T]] = SchemaFor(Schema.createArray(schemaFor.schema))

  given[T](using encoder: Encoder[T]): Encoder[NonEmptyList[T]] = new Encoder[NonEmptyList[T]] :
    override def encode(schema: Schema): NonEmptyList[T] => Any = {
      require(schema.getType == Schema.Type.ARRAY)
      val encode = encoder.encode(schema)
      { value => value.map(encode).toList.asJava }
    }

  given[T](using encoder: Encoder[T]): Encoder[NonEmptyVector[T]] = new Encoder[NonEmptyVector[T]] :
    override def encode(schema: Schema): NonEmptyVector[T] => Any = {
      require(schema.getType == Schema.Type.ARRAY)
      val encode = encoder.encode(schema)
      { value => value.map(encode).toVector.asJava }
    }

  given[T](using encoder: Encoder[T]): Encoder[NonEmptyChain[T]] = new Encoder[NonEmptyChain[T]] :
    override def encode(schema: Schema): NonEmptyChain[T] => Any = {
      require(schema.getType == Schema.Type.ARRAY)
      val encode = encoder.encode(schema)
      { value => value.map(encode).toNonEmptyList.toList.asJava }
    }

  given[T](using decoder: Decoder[T]): Decoder[NonEmptyList[T]] = new Decoder[NonEmptyList[T]] :
    override def decode(schema: Schema): Any => NonEmptyList[T] = {
      require(schema.getType == Schema.Type.ARRAY)
      val decode = decoder.decode(schema)
      { value =>
        value match {
          case array: Array[_] => NonEmptyList.fromListUnsafe(array.toList.map(decode))
          case list: java.util.Collection[_] => NonEmptyList.fromListUnsafe(list.asScala.map(decode).toList)
          case other => sys.error("Unsupported type " + other)
        }
      }
    }

  given[T](using decoder: Decoder[T]): Decoder[NonEmptyVector[T]] = new Decoder[NonEmptyVector[T]] :
    override def decode(schema: Schema): Any => NonEmptyVector[T] = {
      require(schema.getType == Schema.Type.ARRAY)
      val decode = decoder.decode(schema)
      { value =>
        value match {
          case array: Array[_] => NonEmptyVector.fromVectorUnsafe(array.toVector.map(decode))
          case list: java.util.Collection[_] => NonEmptyVector.fromVectorUnsafe(list.asScala.map(decode).toVector)
          case other => sys.error("Unsupported type " + other)
        }
      }
    }

  given[T](using decoder: Decoder[T]): Decoder[NonEmptyChain[T]] = new Decoder[NonEmptyChain[T]] :
    override def decode(schema: Schema): Any => NonEmptyChain[T] = {
      require(schema.getType == Schema.Type.ARRAY)
      val decode = decoder.decode(schema)
      { value =>
        value match {
          case array: Array[_] => NonEmptyChain.fromSeq(array.toList.map(decode)).get
          case list: java.util.Collection[_] => NonEmptyChain.fromSeq(list.asScala.map(decode).toList).get
          case other => sys.error("Unsupported type " + other)
        }
      }
    }
