package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.Schema.Field

object AvroSchemaMerge {

  import scala.collection.JavaConverters._

  def apply(name: String, namespace: String, schemas: List[Schema]): Schema = {
    require(schemas.forall(_.getType == Schema.Type.RECORD), "Can only merge records")

    val doc = schemas.flatMap(x => Option(x.getDoc)).mkString("; ")

    val fields = schemas.flatMap(_.getFields.asScala).groupBy(_.name).map { case (name, fields) =>

        val doc = fields.flatMap(x => Option(x.doc)).mkString("; ")
        val default = fields.find(_.defaultVal != null).map(_.defaultVal).orNull

        // if we have two schemas with the same type, then just keep the first one
        val union = {
          val schemas = fields.map(_.schema).groupBy(_.getType).map(_._2.head).toList.sortBy(_.getName) :+ Schema.create(Schema.Type.NULL)
          Schema.createUnion(schemas.asJava)
        }

        new Field(name, union, if (doc.isEmpty) null else doc, default)
    }

    val schema = Schema.createRecord(name, if (doc.isEmpty) null else doc, namespace, false)
    schema.setFields(fields.toList.asJava)
    schema
  }
}