package benchmarks

import benchmarks.record.AttributeValue
import benchmarks.record.AttributeValue.{Empty, Invalid, Valid}
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.{TypeTag, typeOf}

package object handrolled_codecs {

  final class AttributeValueCodec[T: Codec](val schemaForValid: SchemaForV2[Valid[T]]) extends Codec[AttributeValue[T]] {


    def schemaFor: SchemaForV2[AttributeValue[T]] = {
      implicit val sfv: SchemaForV2[Valid[T]] = schemaForValid
      SchemaForV2[AttributeValue[T]]
    }

    def unionSchemaElementWhere(predicate: Schema => Boolean): Schema = schema.getTypes.asScala.find(predicate).get

    val validCodec = Codec[Valid[T]].withSchema(schemaForValid)
    val emptyCodec = Codec[Empty]
    val invalidCodec = Codec[Invalid]

    def encode(t: AttributeValue[T]): AnyRef = t match {
      case v: Valid[T] => validCodec.encode(v)
      case e: Empty    => emptyCodec.encode(e)
      case i: Invalid  => invalidCodec.encode(i)
    }

    val validSn: String = validCodec.schema.getFullName
    val emptySn: String = emptyCodec.schema.getFullName
    val invalidSn: String = invalidCodec.schema.getFullName

    def decode(value: Any): AttributeValue[T] = {
      val schema = value match {
        case r: GenericData.Record => r.getSchema
        case i: ImmutableRecord    => i.schema
      }
      schema.getFullName match {
        case `validSn`   => validCodec.decode(value)
        case `emptySn`   => emptyCodec.decode(value)
        case `invalidSn` => invalidCodec.decode(value)
      }
    }
  }

  def buildSchemaForValid[T: SchemaForV2: TypeTag]: SchemaForV2[Valid[T]] = {
    val sf = SchemaForV2[Valid[T]]
    val name: String = typeOf[T].typeSymbol.name.toString
    val s = sf.schema
    val fields = s.getFields.asScala.map(f => new Schema.Field(f.name, f.schema, f.doc, f.defaultVal)).asJava
    SchemaForV2(Schema.createRecord(s"Valid$name", s.getDoc, s.getNamespace, s.isError, fields), sf.fieldMapper)
  }

  object AttributeValueCodec {
    def apply[T: Codec: SchemaForV2: TypeTag]: AttributeValueCodec[T] = {
      implicit val schemaForValid: SchemaForV2[Valid[T]] = buildSchemaForValid
      new AttributeValueCodec[T](schemaForValid)
    }
  }
}
