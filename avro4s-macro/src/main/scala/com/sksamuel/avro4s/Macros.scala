package com.sksamuel.avro4s

import org.apache.avro.Schema

import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.macros.Context

trait AvroSchemaWriter[T] {
  def schema: org.apache.avro.Schema
}

trait AvroFieldWriter[T] {
  def field(name: String): Schema.Field
}

object SchemaUtils {

  import scala.reflect.runtime.universe._

  def schemaType(sig: String): org.apache.avro.Schema.Type = {
    sig match {
      case "Array[Byte]" | "List[Byte]" | "Seq[Byte]" => org.apache.avro.Schema.Type.BYTES
      case "Array" => org.apache.avro.Schema.Type.ARRAY
      case "Boolean" => org.apache.avro.Schema.Type.BOOLEAN
      case "Double" => org.apache.avro.Schema.Type.DOUBLE
      case "Float" => org.apache.avro.Schema.Type.FLOAT
      case "Int" => org.apache.avro.Schema.Type.INT
      case "Long" => org.apache.avro.Schema.Type.LONG
      case "String" => org.apache.avro.Schema.Type.STRING
      case _ => org.apache.avro.Schema.Type.STRING
    }
  }

  def schemaType(a: Any): Schema = {
    Schema.create(Schema.Type.INT)
  }

  def schemaFields(xs: Seq[(String, FreeTypeSymbol)]): List[org.apache.avro.Schema.Field] = Nil
  //    xs.map { case (name, sig) =>
  //      new Schema.Field(name, Schema.create(schemaType(sig)), s"Generated from scala type: $sig", null)
  //    }.toList
  //  }
}

import scala.language.experimental.macros

object Macros {

  implicit val stringSchema: AvroFieldWriter[String] = new AvroFieldWriter[String] {
    override def field(name: String): Schema.Field = new Schema.Field(name, Schema.create(Schema.Type.STRING), null, null)
  }

  implicit val intSchema: AvroFieldWriter[Int] = new AvroFieldWriter[Int] {
    override def field(name: String): Schema.Field = new Schema.Field(name, Schema.create(Schema.Type.INT), null, null)
  }

  implicit val doubleSchema: AvroFieldWriter[Double] = new AvroFieldWriter[Double] {
    override def field(name: String): Schema.Field = new Schema.Field(name, Schema.create(Schema.Type.DOUBLE), null, null)
  }

  implicit val longSchema: AvroFieldWriter[Long] = new AvroFieldWriter[Long] {
    override def field(name: String): Schema.Field = new Schema.Field(name, Schema.create(Schema.Type.LONG), null, null)
  }

  implicit val booleanSchema: AvroFieldWriter[Boolean] = new AvroFieldWriter[Boolean] {
    override def field(name: String): Schema.Field = new
        Schema.Field(name, Schema.create(Schema.Type.BOOLEAN), "", null)
  }

  //  implicit def optionSchema[T: Schema]: Schema[Option[T]] = new Schema("optional[" + implicitly[Schema[T]] + "]")
  // implicit def listSchema[T: Schema]: Schema[List[T]] = new Schema("list_of[" + implicitly[Schema[T]] + "]")

  implicit def classSchema[T]: AvroSchemaWriter[T] = macro classSchema_impl[T]

  def classSchema_impl[T: c.WeakTypeTag](c: Context): c.Expr[AvroSchemaWriter[T]] = {

    import c.universe._
    val t = weakTypeOf[T]

    val fields = t.declarations.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramss.head

    val name = t.typeSymbol.fullName

    val fieldSchemaPartTrees: Seq[Tree] = fields.map { f =>
      q"""{import Macros._; implicitly[AvroFieldWriter[${f.typeSignature}]].field(${f.name.decoded})}"""
    }

    c.Expr[AvroSchemaWriter[T]]( q"""
      new AvroSchemaWriter[$t] {
        def schema = {
         import scala.collection.JavaConverters._ 
         val s = org.apache.avro.Schema.createRecord($name, null, $name, false)
         val fields = Seq(..$fieldSchemaPartTrees)
         s.setFields(fields.asJava)
         s
        }
      }
    """)
  }
}