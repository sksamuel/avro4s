package benchmarks

import benchmarks.record.AttributeValue
import benchmarks.record.AttributeValue.{Empty, Invalid, Valid}
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.{TypeTag, typeOf}

package object handrolled_codecs {

  final class AttributeValueCodec[T: Encoder: Decoder](val schemaFor: SchemaFor[AttributeValue[T]])
      extends Encoder[AttributeValue[T]]
      with Decoder[AttributeValue[T]]
      with SchemaFor[AttributeValue[T]] {

    val schema: Schema = AvroSchema[AttributeValue[T]](schemaFor)

    def unionSchemaElementWhere(predicate: Schema => Boolean): Schema = schema.getTypes.asScala.find(predicate).get

    val validEncoder: Encoder[Valid[T]] = Encoder[Valid[T]]
    val validDecoder: Decoder[Valid[T]] = Decoder[Valid[T]]
    val emptyEncoder: Encoder[Empty] = Encoder[Empty]
    val emptyDecoder: Decoder[Empty] = Decoder[Empty]
    val invalidEncoder: Encoder[Invalid] = Encoder[Invalid]
    val invalidDecoder: Decoder[Invalid] = Decoder[Invalid]

    val validSchema: Schema = {
      val typeName = typeOf[Valid[_]].typeSymbol.name.toString
      unionSchemaElementWhere(_.getName.startsWith(typeName))
    }

    val emptySchema: Schema = {
      val typeName = typeOf[Empty].typeSymbol.name.toString
      unionSchemaElementWhere(_.getName == typeName)
    }

    val invalidSchema: Schema = {
      val typeName = typeOf[Invalid].typeSymbol.name.toString
      unionSchemaElementWhere(_.getName == typeName)
    }

    def encode(t: AttributeValue[T], schema: Schema, fieldMapper: FieldMapper): AnyRef = t match {
      case v: Valid[T] => validEncoder.encode(v, validSchema, fieldMapper)
      case e: Empty    => emptyEncoder.encode(e, emptySchema, fieldMapper)
      case i: Invalid  => invalidEncoder.encode(i, invalidSchema, fieldMapper)
    }

    val validSn: String = validSchema.getFullName
    val emptySn: String = emptySchema.getFullName
    val invalidSn: String = invalidSchema.getFullName

    def decode(value: Any, schema: Schema, fieldMapper: FieldMapper): AttributeValue[T] = {
      val schema = value match {
        case r: GenericData.Record => r.getSchema
        case i: ImmutableRecord    => i.schema
      }
      schema.getFullName match {
        case `validSn`   => validDecoder.decode(value, validSchema, fieldMapper)
        case `emptySn`   => emptyDecoder.decode(value, emptySchema, fieldMapper)
        case `invalidSn` => invalidDecoder.decode(value, invalidSchema, fieldMapper)
      }
    }

    def schema(fieldMapper: FieldMapper): Schema = schemaFor.schema(fieldMapper)
  }

  def buildSchemaForValid[T: SchemaFor: TypeTag]: SchemaFor[Valid[T]] = {
    val sf: SchemaFor[Valid[T]] = SchemaFor[Valid[T]]
    val name: String = typeOf[T].typeSymbol.name.toString
    fieldMapper =>
      {
        val s = sf.schema(fieldMapper)
        val fields = s.getFields.asScala.map(f => new Schema.Field(f.name, f.schema, f.doc, f.defaultVal)).asJava
        Schema.createRecord(s"Valid$name", s.getDoc, s.getNamespace, s.isError, fields)
      }
  }

  object AttributeValueCodec {
    def apply[T: Encoder: Decoder: SchemaFor: TypeTag]: AttributeValueCodec[T] = {
      implicit val schemaForValid: SchemaFor[Valid[T]] = buildSchemaForValid
      val schemaFor = SchemaFor[AttributeValue[T]]
      new AttributeValueCodec[T](schemaFor)
    }
  }
}
