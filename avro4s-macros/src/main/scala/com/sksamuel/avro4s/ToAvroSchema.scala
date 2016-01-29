package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema

import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.macros.Context

trait ToAvroSchema[T] {
  def apply(): org.apache.avro.Schema
}

case class Anno(classname: String, values: Seq[String])

object ToAvroSchema {

  implicit val BooleanSchema: ToAvroSchema[Boolean] = new ToAvroSchema[Boolean] {
    def apply(): Schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit val BigDecimalSchema: ToAvroSchema[BigDecimal] = new ToAvroSchema[BigDecimal] {
    def apply(): Schema = {
      val schema = Schema.create(Schema.Type.BYTES)
      schema.addProp("logicalType", "decimal")
      schema.addProp("scale", "2")
      schema.addProp("precision", "8")
      schema
    }
  }

  implicit val ByteArraySchema: ToAvroSchema[Array[Byte]] = new ToAvroSchema[Array[Byte]] {
    def apply(): Schema = Schema.create(Schema.Type.BYTES)
  }

  implicit val DoubleSchema: ToAvroSchema[Double] = new ToAvroSchema[Double] {
    def apply(): Schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit def EitherSchema[A, B](implicit aSchema: ToAvroSchema[A], bSchema: ToAvroSchema[B]): ToAvroSchema[Either[A, B]] = {
    new ToAvroSchema[Either[A, B]] {
      def apply(): Schema = Schema.createUnion(util.Arrays.asList(aSchema.apply, bSchema.apply))
    }
  }

  implicit def EnumToSchema[E <: Enum[E]](implicit tag: ClassTag[E]): ToAvroSchema[Enum[E]] = new ToAvroSchema[Enum[E]] {
    override def apply(): Schema = {
      import scala.collection.JavaConverters._
      val values = tag.runtimeClass.getEnumConstants.map(_.toString)
      Schema.createEnum(tag.runtimeClass.getSimpleName, null, tag.runtimeClass.getPackage.getName, values.toList.asJava)
    }
  }

  implicit val FloatSchema: ToAvroSchema[Float] = new ToAvroSchema[Float] {
    def apply(): Schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit val IntSchema: ToAvroSchema[Int] = new ToAvroSchema[Int] {
    def apply(): Schema = Schema.create(Schema.Type.INT)
  }

  implicit val LongSchema: ToAvroSchema[Long] = new ToAvroSchema[Long] {
    def apply(): Schema = Schema.create(Schema.Type.LONG)
  }

  implicit val StringSchema: ToAvroSchema[String] = new ToAvroSchema[String] {
    def apply(): Schema = Schema.create(Schema.Type.STRING)
  }

  implicit def OptionSchema[T](implicit valueSchema: ToAvroSchema[T]): ToAvroSchema[Option[T]] = {
    new ToAvroSchema[Option[T]] {
      def apply(): Schema = Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), valueSchema.apply))
    }
  }

  implicit def ArraySchema[S](implicit subschema: ToAvroSchema[S]): ToAvroSchema[Array[S]] = {
    new ToAvroSchema[Array[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def IterableSchema[S](implicit subschema: ToAvroSchema[S]): ToAvroSchema[Iterable[S]] = {
    new ToAvroSchema[Iterable[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def ListSchema[S](implicit subschema: ToAvroSchema[S]): ToAvroSchema[List[S]] = {
    new ToAvroSchema[List[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def SetSchema[S](implicit subschema: ToAvroSchema[S]): ToAvroSchema[Set[S]] = {
    new ToAvroSchema[Set[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def SeqSchema[S](implicit subschema: ToAvroSchema[S]): ToAvroSchema[Seq[S]] = {
    new ToAvroSchema[Seq[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def MapSchema[V](implicit valueSchema: ToAvroSchema[V]): ToAvroSchema[Map[String, V]] = {
    new ToAvroSchema[Map[String, V]] {
      def apply(): Schema = Schema.createMap(valueSchema.apply)
    }
  }

  def annotationsFor(klass: Class[_], annos: Seq[Anno]): Seq[Anno] = annos.filter(_.classname == klass.getName)

  def doc(annos: Seq[Anno]): String = {
    annos.find(_.classname == classOf[AvroDoc].getName).flatMap(_.values.headOption).orNull
  }

  def aliases(annos: Seq[Anno]): Seq[String] = annotationsFor(classOf[AvroAlias], annos).flatMap(_.values.headOption)

  def addProps(annos: Seq[Anno], f: (String, String) => Unit): Unit = {
    annotationsFor(classOf[AvroProp], annos).map(_.values.toList).foreach {
      case key :: value :: Nil => f(key, value)
    }
  }

  def fieldBuilder[T](name: String, annos: Seq[Anno])(implicit schema: ToAvroSchema[T]): Schema.Field = {
    val field = new Schema.Field(name, schema.apply, doc(annos), null)
    aliases(annos).foreach(field.addAlias)
    addProps(annos, field.addProp)
    field
  }

  def schemaBuilder[T](name: String, pack: String, fields: Seq[Schema.Field], annos: Seq[Anno]): Schema = {
    import scala.collection.JavaConverters._
    val schema = org.apache.avro.Schema.createRecord(name, doc(annos), pack, false)
    addProps(annos, schema.addProp)
    schema.setFields(fields.asJava)
    schema
  }

  implicit def apply[T]: ToAvroSchema[T] = macro impl[T]

  def impl[T: c.WeakTypeTag](c: Context): c.Expr[ToAvroSchema[T]] = {
    import c.universe._

    def annotations(sym: Symbol): Seq[c.Tree] = sym.annotations.map {
      a =>
        val name = a.tpe.typeSymbol.fullName
        val args = a.scalaArgs.map(_.toString.stripPrefix("\"").stripSuffix("\""))
        q"com.sksamuel.avro4s.Anno($name, $args)"
    }

    val tpe = weakTypeOf[T]
    require(tpe.typeSymbol.isClass, tpe + " is not a class")

    val name = tpe.typeSymbol.name.decoded
    val pack = tpe.typeSymbol.fullName.split('.').takeWhile(_.forall(c => !c.isUpper)).mkString(".")
    val annos = annotations(tpe.typeSymbol)

    val fields = tpe.declarations.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.flatMap(_.paramss.headOption).getOrElse(Nil)

    val fieldSchemaPartTrees: Seq[Tree] = fields.map {
      f =>
        val name = f.name.decoded
        val sig = f.typeSignature
        val annos = annotations(f)
        q"""{import com.sksamuel.avro4s.ToAvroSchema._; fieldBuilder[$sig]($name, Seq(..$annos))}"""
    }

    c.Expr[ToAvroSchema[T]](
      q"""new ToAvroSchema[$tpe] {
            def apply() = {
              com.sksamuel.avro4s.ToAvroSchema.schemaBuilder[$tpe](
                $name,
                $pack,
                Seq(..$fieldSchemaPartTrees),
                Seq(..$annos)
              )
            }
          }
        """
    )
  }
}