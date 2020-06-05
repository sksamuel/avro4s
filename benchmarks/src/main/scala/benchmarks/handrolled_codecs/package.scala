package benchmarks

import benchmarks.record.AttributeValue
import benchmarks.record.AttributeValue.{Empty, Invalid, Valid}
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.{TypeTag, typeOf}

package object handrolled_codecs {

  final class AttributeValueCodec[T: Encoder: Decoder](val schemaForValid: SchemaFor[Valid[T]])
      extends Codec[AttributeValue[T]] { codec =>

    def schemaFor: SchemaFor[AttributeValue[T]] = {
      implicit val sfv: SchemaFor[Valid[T]] = schemaForValid
      SchemaFor[AttributeValue[T]]
    }

    val validEncoder = Encoder[Valid[T]].withSchema(schemaForValid)
    val emptyEncoder = Encoder[Empty]
    val invalidEncoder = Encoder[Invalid]

    def encode(t: AttributeValue[T]): AnyRef = t match {
      case v: Valid[T] => validEncoder.encode(v)
      case e: Empty    => emptyEncoder.encode(e)
      case i: Invalid  => invalidEncoder.encode(i)
    }

    val validDecoder = Decoder[Valid[T]].withSchema(schemaForValid)
    val emptyDecoder = Decoder[Empty]
    val invalidDecoder = Decoder[Invalid]

    val validSn: String = validDecoder.schema.getFullName
    val emptySn: String = emptyDecoder.schema.getFullName
    val invalidSn: String = invalidDecoder.schema.getFullName

    def decode(value: Any): AttributeValue[T] = {
      val schema = value match {
        case r: GenericData.Record => r.getSchema
        case i: ImmutableRecord    => i.schema
      }
      schema.getFullName match {
        case `validSn`   => validDecoder.decode(value)
        case `emptySn`   => emptyDecoder.decode(value)
        case `invalidSn` => invalidDecoder.decode(value)
      }
    }
  }

  def buildSchemaForValid[T: SchemaFor: TypeTag]: SchemaFor[Valid[T]] = {
    val sf = SchemaFor[Valid[T]]
    val name: String = typeOf[T].typeSymbol.name.toString
    val s = sf.schema
    val fields = s.getFields.asScala.map(f => new Schema.Field(f.name, f.schema, f.doc, f.defaultVal)).asJava
    SchemaFor(Schema.createRecord(s"Valid$name", s.getDoc, s.getNamespace, s.isError, fields), sf.fieldMapper)
  }

  object AttributeValueCodec {
    def apply[T: Encoder: Decoder: SchemaFor: TypeTag]: AttributeValueCodec[T] = {
      implicit val schemaForValid: SchemaFor[Valid[T]] = buildSchemaForValid
      new AttributeValueCodec[T](schemaForValid)
    }
  }
}
