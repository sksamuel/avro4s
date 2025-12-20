package com.sksamuel.avro4s

import _root_.cats.data.{NonEmptyList, NonEmptyVector, NonEmptyChain, Validated, ValidatedNel}
import org.apache.avro.Schema
import com.sksamuel.avro4s.avroutils.SchemaHelper
import com.sksamuel.avro4s.ImmutableRecord
import scala.language.implicitConversions
import org.apache.avro.generic.GenericContainer
import scala.reflect.ClassTag
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

package object cats:

  import scala.collection.JavaConverters._

  given[T](using schemaFor: SchemaFor[T]): SchemaFor[NonEmptyList[T]] = SchemaFor(Schema.createArray(schemaFor.schema))
  given[T](using schemaFor: SchemaFor[T]): SchemaFor[NonEmptyVector[T]] = SchemaFor(Schema.createArray(schemaFor.schema))
  given[T](using schemaFor: SchemaFor[T]): SchemaFor[NonEmptyChain[T]] = SchemaFor(Schema.createArray(schemaFor.schema))

  given[T](using encoder: Encoder[T], schemaFor: SchemaFor[T]): Encoder[NonEmptyList[T]] = new Encoder[NonEmptyList[T]] :
    override def encode(schema: Schema): NonEmptyList[T] => AnyRef = {
      require(schema.getType == Schema.Type.ARRAY)
      val encode = encoder.encode(schemaFor.schema)
      { value => value.map(encode).toList.asJava }
    }

  given[T](using encoder: Encoder[T], schemaFor: SchemaFor[T]): Encoder[NonEmptyVector[T]] = new Encoder[NonEmptyVector[T]] :
    override def encode(schema: Schema): NonEmptyVector[T] => AnyRef = {
      require(schema.getType == Schema.Type.ARRAY)
      val encode = encoder.encode(schemaFor.schema)
      { value => value.map(encode).toVector.asJava }
    }

  given[T](using encoder: Encoder[T], schemaFor: SchemaFor[T]): Encoder[NonEmptyChain[T]] = new Encoder[NonEmptyChain[T]] :
    override def encode(schema: Schema): NonEmptyChain[T] => AnyRef = {
      require(schema.getType == Schema.Type.ARRAY)
      val encode = encoder.encode(schemaFor.schema)
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

  extension (name: String)
    def removeSpecialCharacters: String = name.replace('$', '.').replace(".", "_")

  given [E, T] (using eSchemaFor: SchemaFor[E], eClassTag: ClassTag[E], tSchemaFor: SchemaFor[T], tClassTag: ClassTag[T]): SchemaFor[Validated[E, T]] = 
    val schemaNameforInvalid = s"InvalidWrapper__${eClassTag}".removeSpecialCharacters
    val schemaForInvalid = Schema.createRecord(name = schemaNameforInvalid, doc = null, namespace = "com.sksamuel.avro4s.cats", isError = false)
    schemaForInvalid.setFields(Seq(new Schema.Field("value", eSchemaFor.schema)).asJava)

    val schemaNameforValid = s"ValidWrapper__${tClassTag}".removeSpecialCharacters
    val schemaForValid = Schema.createRecord(name = schemaNameforValid, doc = null, namespace = "com.sksamuel.avro4s.cats", isError = false)
    schemaForValid.setFields(Seq(new Schema.Field("value", tSchemaFor.schema)).asJava)
    SchemaFor(
      Schema.createUnion(
        List(
          schemaForInvalid,
          schemaForValid
        ).asJava
      )
    )

  given [E, T] (using eEncoder: Encoder[E], eSchemaFor: SchemaFor[E], tEncoder: Encoder[T], tSchemaFor: SchemaFor[T]): Encoder[Validated[E, T]] = 
    new Encoder[Validated[E, T]] {
      override def encode(schema: Schema): Validated[E, T] => AnyRef = {
        require(schema.getType == Schema.Type.UNION)
        val invalidSchema = SchemaHelper.getFirstFromUnionOfTwo(schema, "InvalidWrapper")
        val validSchema = SchemaHelper.getSecondFromUnionOfTwo(schema, "ValidWrapper")
        
        {
          case Validated.Invalid(e) => 
            val recordForInvalid = GenericData.Record(invalidSchema)
            val encodeE = eEncoder.encode(eSchemaFor.schema)
            recordForInvalid.put("value", encodeE.apply(e))
            recordForInvalid
          case Validated.Valid(t) => 
            val recordForValid = GenericData.Record(validSchema)
            val encodeT = tEncoder.encode(tSchemaFor.schema)
            recordForValid.put("value", encodeT.apply(t))
            recordForValid
        }
      }
    }

  given [E, T] (using eClassTag: ClassTag[E], eDecoder: Decoder[E], eSchemaFor: SchemaFor[E], tClassTag: ClassTag[T], tDecoder: Decoder[T], tSchemaFor: SchemaFor[T]): Decoder[Validated[E, T]] = 
    new Decoder[Validated[E, T]] {
      override def decode(schema: Schema): Any => Validated[E, T] = {
        require(schema.getType == Schema.Type.UNION)

        value => {
          value match {
            case v: GenericContainer if v.getSchema.getName.startsWith("InvalidWrapper") => 
              val invalidValue = v.asInstanceOf[GenericRecord].get("value")
              Validated.Invalid(eDecoder.decode(eSchemaFor.schema)(invalidValue))
            case v: GenericContainer if v.getSchema.getName.startsWith("ValidWrapper") =>  
              val validValue = v.asInstanceOf[GenericRecord].get("value")
              Validated.Valid(tDecoder.decode(tSchemaFor.schema)(validValue))
            case _ =>
              throw new Avro4sDecodingException(
                s"Could not decode $value into cats.data.Validated[${eClassTag}, ${tClassTag}]",
                value
              )
          }
        }
      }
    }
