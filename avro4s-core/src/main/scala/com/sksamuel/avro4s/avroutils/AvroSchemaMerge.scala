package com.sksamuel.avro4s.avroutils

import org.apache.avro.Schema.Field
import org.apache.avro.{JsonProperties, Schema}

object AvroSchemaMerge {

  import scala.collection.JavaConverters._

  def apply(name: String, namespace: String, schemas: List[Schema]): Schema = {
    // should this also be converted to throw an Avro4sExcpeption?
    require(schemas.forall(_.getType == Schema.Type.RECORD), "Can only merge records")

    val doc = schemas.flatMap(x => Option(x.getDoc)).mkString("; ")

    val fields = schemas.flatMap(_.getFields.asScala).groupBy(_.name).map { case (name, fields) =>

      val doc = fields.flatMap(x => Option(x.doc)).mkString("; ")
      val default = fields.find(_.defaultVal != null).map(_.defaultVal).orNull

      // if we have two schemas with the same type, then just keep the first one
      val union = {
        val schemas = fields
          .map(_.schema)
          .flatMap(schema => schema.getType match {
            case Schema.Type.UNION => schema.getTypes.asScala
            case _ => Seq(schema)
          })
          .filter(_.getType != Schema.Type.NULL)
          .groupBy(_.getType)
          .map(_._2.head)
          .toList
          .sortBy(_.getName)

        // if default value was not specified or equal to JsonProperties.NULL_VALUE then null schema should be the first in union
        Schema.createUnion({
          if (default == null || default == JsonProperties.NULL_VALUE) {
            (Schema.create(Schema.Type.NULL) :: schemas).asJava
          } else {
            (schemas :+ Schema.create(Schema.Type.NULL)).asJava
          }
        })
      }

      new Field(name, union, if (doc.isEmpty) null else doc, default)
    }

    val schema = Schema.createRecord(name, if (doc.isEmpty) null else doc, namespace, false)
    schema.setFields(fields.toList.asJava)
    schema
  }
}
