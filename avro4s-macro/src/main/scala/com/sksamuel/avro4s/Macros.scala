package com.sksamuel.avro4s

import org.apache.avro.Schema

import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.macros.Context

trait AvroSchemaWriter[T] {
  def schema: org.apache.avro.Schema
  def props: Map[String, String] = Map.empty
}

trait AvroFieldWriter[T] {
  def field(name: String): Schema.Field
}

object Macros {

  implicit val StringSchema: AvroSchemaWriter[String] = new AvroSchemaWriter[String] {
    def schema: Schema = Schema.create(Schema.Type.STRING)
  }

  implicit val IntSchema: AvroSchemaWriter[Int] = new AvroSchemaWriter[Int] {
    def schema: Schema = Schema.create(Schema.Type.INT)
  }

  implicit val LongSchema: AvroSchemaWriter[Long] = new AvroSchemaWriter[Long] {
    def schema: Schema = Schema.create(Schema.Type.LONG)
  }

  implicit val BooleanSchema: AvroSchemaWriter[Boolean] = new AvroSchemaWriter[Boolean] {
    def schema: Schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit val FloatSchema: AvroSchemaWriter[Float] = new AvroSchemaWriter[Float] {
    def schema: Schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit val ByteArraySchema: AvroSchemaWriter[Array[Byte]] = new AvroSchemaWriter[Array[Byte]] {
    def schema: Schema = Schema.create(Schema.Type.BYTES)
  }

  implicit val DoubleSchema: AvroSchemaWriter[Double] = new AvroSchemaWriter[Double] {
    def schema: Schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit val BigDecimalSchema: AvroSchemaWriter[BigDecimal] = new AvroSchemaWriter[BigDecimal] {
    def schema: Schema = Schema.create(Schema.Type.DOUBLE)
    override def props: Map[String, String] = Map("logicalType" -> "decimal", "precision" -> "4", "scale" -> "2")
  }

  implicit def ArraySchema[S](implicit subschema: AvroSchemaWriter[S]): AvroSchemaWriter[Array[S]] = {
    new AvroSchemaWriter[Array[S]] {
      def schema: Schema = Schema.createArray(subschema.schema)
    }
  }

  implicit def IterableSchema[S](implicit subschema: AvroSchemaWriter[S]): AvroSchemaWriter[Iterable[S]] = {
    new AvroSchemaWriter[Iterable[S]] {
      def schema: Schema = Schema.createArray(subschema.schema)
    }
  }

  implicit def ListSchema[S](implicit subschema: AvroSchemaWriter[S]): AvroSchemaWriter[List[S]] = {
    new AvroSchemaWriter[List[S]] {
      def schema: Schema = Schema.createArray(subschema.schema)
    }
  }

  implicit def SeqSchema[S](implicit subschema: AvroSchemaWriter[S]): AvroSchemaWriter[Seq[S]] = {
    new AvroSchemaWriter[Seq[S]] {
      def schema: Schema = Schema.createArray(subschema.schema)
    }
  }

  implicit def MapSchema[V](implicit valueSchema: AvroSchemaWriter[V]): AvroSchemaWriter[Map[String, V]] = {
    new AvroSchemaWriter[Map[String, V]] {
      def schema: Schema = Schema.createMap(valueSchema.schema)
    }
  }

  def fieldBuilder[T](name: String)(implicit schema: AvroSchemaWriter[T]): Schema.Field = {
    val field = new Schema.Field(name, schema.schema, null, null)
    schema.props.foreach { case (k, v) => field.addProp(k, v) }
    field
  }

  def schemaImpl[T: c.WeakTypeTag](c: Context): c.Expr[AvroSchemaWriter[T]] = {

    import c.universe._
    val t = weakTypeOf[T]

    val fields = t.declarations.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramss.head

    val name = t.typeSymbol.fullName

    val fieldSchemaPartTrees: Seq[Tree] = fields.map { f =>
      val name = f.name.decoded
      val sig = f.typeSignature
      q"""{import Macros._; fieldBuilder[$sig]($name)}"""
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