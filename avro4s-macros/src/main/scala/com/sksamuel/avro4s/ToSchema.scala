package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema

import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.macros.Context

object ToSchemaMacros {

  def apply[T]: ToSchema[T] = macro impl[T]

  def impl[T: c.WeakTypeTag](c: Context): c.Expr[ToSchema[T]] = {
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
        q"""{import com.sksamuel.avro4s.ToSchema._; fieldBuilder[$sig]($name, Seq(..$annos))}"""
    }

    c.Expr[ToSchema[T]](
      q"""new ToSchema[$tpe] {
            def apply() = {
              com.sksamuel.avro4s.ToSchema.schemaBuilder[$tpe](
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

trait ToSchema[T] {
  def apply(): org.apache.avro.Schema
}

trait LowPriorityToSchema {

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

  def fieldBuilder[T](name: String, annos: Seq[Anno])(implicit schema: ToSchema[T]): Schema.Field = {
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

  implicit def GenericToSchema[T]: ToSchema[T] = ToSchemaMacros.apply[T]
}

object HighPriorityToSchema extends LowPriorityToSchema {

  implicit val BooleanToSchema: ToSchema[Boolean] = new ToSchema[Boolean] {
    def apply(): Schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit val BigDecimalToSchema: ToSchema[BigDecimal] = new ToSchema[BigDecimal] {
    def apply(): Schema = {
      val schema = Schema.create(Schema.Type.BYTES)
      schema.addProp("logicalType", "decimal")
      schema.addProp("scale", "2")
      schema.addProp("precision", "8")
      schema
    }
  }

  implicit val ByteArraySchema: ToSchema[Array[Byte]] = new ToSchema[Array[Byte]] {
    def apply(): Schema = Schema.create(Schema.Type.BYTES)
  }

  implicit val DoubleToSchema: ToSchema[Double] = new ToSchema[Double] {
    def apply(): Schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit def EitherSchema[A, B](implicit aSchema: ToSchema[A], bSchema: ToSchema[B]): ToSchema[Either[A, B]] = {
    new ToSchema[Either[A, B]] {
      def apply(): Schema = Schema.createUnion(util.Arrays.asList(aSchema.apply, bSchema.apply))
    }
  }

  implicit def EnumToSchema[E <: Enum[E]](implicit tag: ClassTag[E]): ToSchema[Enum[E]] = new ToSchema[Enum[E]] {
    override def apply(): Schema = {
      import scala.collection.JavaConverters._
      val values = tag.runtimeClass.getEnumConstants.map(_.toString)
      Schema.createEnum(tag.runtimeClass.getSimpleName, null, tag.runtimeClass.getPackage.getName, values.toList.asJava)
    }
  }

  implicit val FloatSchema: ToSchema[Float] = new ToSchema[Float] {
    def apply(): Schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit val IntSchema: ToSchema[Int] = new ToSchema[Int] {
    def apply(): Schema = Schema.create(Schema.Type.INT)
  }

  implicit val LongSchema: ToSchema[Long] = new ToSchema[Long] {
    def apply(): Schema = Schema.create(Schema.Type.LONG)
  }

  implicit val StringToSchema: ToSchema[String] = new ToSchema[String] {
    def apply(): Schema = Schema.create(Schema.Type.STRING)
  }

  implicit def OptionToSchema[T](implicit valueSchema: ToSchema[T]): ToSchema[Option[T]] = {
    new ToSchema[Option[T]] {
      def apply(): Schema = Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), valueSchema.apply))
    }
  }

  implicit def ArrayToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Array[S]] = {
    new ToSchema[Array[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def IterableToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Iterable[S]] = {
    new ToSchema[Iterable[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def ListToSchema[S](implicit subschema: ToSchema[S]): ToSchema[List[S]] = {
    new ToSchema[List[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def SetToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Set[S]] = {
    new ToSchema[Set[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def SeqToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Seq[S]] = {
    new ToSchema[Seq[S]] {
      def apply(): Schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def MapToSchema[V](implicit valueSchema: ToSchema[V]): ToSchema[Map[String, V]] = {
    new ToSchema[Map[String, V]] {
      def apply(): Schema = Schema.createMap(valueSchema.apply)
    }
  }
}

case class Anno(classname: String, values: Seq[String])
