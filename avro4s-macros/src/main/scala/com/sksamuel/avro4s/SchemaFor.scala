package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema

import scala.annotation.implicitNotFound
import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.macros.Context
import scala.collection.JavaConverters._

@implicitNotFound(msg = "Cannot find implicit ToSchema for ${T}")
trait ToSchema[T] {
  def apply(): org.apache.avro.Schema
}

trait LowPriorityToSchema {

  implicit def GenericToSchema[T: Manifest](implicit schemaFor: SchemaFor[T]): ToSchema[T] = new ToSchema[T] {
    override def apply(): Schema = schemaFor()
  }
}

object ToSchema extends LowPriorityToSchema {

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

  implicit def EnumToSchema[E <: Enum[_]](implicit tag: ClassTag[E]): ToSchema[E] = new ToSchema[E] {
    override def apply(): Schema = {
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

@implicitNotFound("Cout not find implicit SchemaFor[${T}]")
trait SchemaFor[T] {
  def apply(): org.apache.avro.Schema
}

object SchemaFor {

  implicit def apply[T]: SchemaFor[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: Context): c.Expr[SchemaFor[T]] = {
    import c.universe._

    def annotations(sym: Symbol): Seq[c.Tree] = sym.annotations.map { a =>
      val name = a.tpe.typeSymbol.fullName
      val args = a.scalaArgs.map(_.toString.stripPrefix("\"").stripSuffix("\""))
      q"com.sksamuel.avro4s.Anno($name, $args)"
    }

    def fieldsForType(tpe: c.universe.Type): List[c.universe.Symbol] = {
      tpe.declarations.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m
      }.flatMap(_.paramss.headOption).getOrElse(Nil)
    }

    val tpe = weakTypeTag[T].tpe
    require(tpe.typeSymbol.isClass, tpe + " is not a class but is " + tpe.typeSymbol.fullName)
    val sealedTraitOrClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isSealed

    val fieldSchemaPartTrees: Seq[Tree] = if (sealedTraitOrClass) {
      val internal = tpe.typeSymbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
      val descendants = internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - tpe.typeSymbol
      descendants.flatMap(x => fieldsForType(x.asType.toType)).groupBy(_.name.decoded).map { case (name, fs) =>
        val schemas = fs map { f =>
          val sig = f.typeSignature
          q"""{
            import com.sksamuel.avro4s.ToSchema._;
            com.sksamuel.avro4s.SchemaFor.schemaBuilder[$sig]
          }
           """
        }
        // if we have the same number of schemas as the number of descendants then it means every subclass
        // of the trait has that particular field, so we can make it non optional, otherwise it means at least
        // one subclass of the trait is missing the field, and so it must be marked optional
        val optional = schemas.size != descendants.size
        q"""{
             com.sksamuel.avro4s.SchemaFor.unionBuilder($name, Set(..$schemas), $optional)
            }
         """
      }.toSeq
    } else {
      fieldsForType(tpe).map { f =>
        val name = f.name.decoded
        val sig = f.typeSignature
        val annos = annotations(f)
        q"""{
            import com.sksamuel.avro4s.ToSchema._;
            com.sksamuel.avro4s.SchemaFor.fieldBuilder[$sig]($name, Seq(..$annos))
          }
       """
      }
    }

    // name of the actual class
    val name = tpe.typeSymbol.name.decoded
    // name of the outer package, can't find a way to get this explicitly so hacking the full class name
    val pack = tpe.typeSymbol.fullName.split('.').takeWhile(_.forall(c => !c.isUpper)).mkString(".")
    val annos = annotations(tpe.typeSymbol)

    c.Expr[SchemaFor[T]](
      q"""new com.sksamuel.avro4s.SchemaFor[$tpe] {
            def apply(): org.apache.avro.Schema = {
              com.sksamuel.avro4s.SchemaFor.recordBuilder[$tpe](
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

  def annotationsFor(klass: Class[_], annos: Seq[Anno]): Seq[Anno] = annos.filter(_.classname == klass.getName)

  def doc(annos: Seq[Anno]): String = {
    annos.find(_.classname == classOf[AvroDoc].getName).flatMap(_.values.headOption).orNull
  }

  // returns any aliases present in the list of annotations
  def aliases(annos: Seq[Anno]): Seq[String] = annotationsFor(classOf[AvroAlias], annos).flatMap(_.values.headOption)

  def addProps(annos: Seq[Anno], f: (String, String) => Unit): Unit = {
    annotationsFor(classOf[AvroProp], annos).map(_.values.toList).foreach {
      case key :: value :: Nil => f(key, value)
      case other => sys.error("Invalid annotation value " + other)
    }
  }

  // builds a schema for a type T
  def schemaBuilder[T](implicit toSchema: ToSchema[T]): Schema = toSchema.apply

  def unionBuilder(name: String, schemas: Set[Schema], optional: Boolean): Schema.Field = {
    val sortedSchemas = (if (optional) schemas + Schema.create(Schema.Type.NULL) else schemas).toSeq.sortBy(_.getName)
    fieldBuilder(name, Nil, Schema.createUnion(sortedSchemas.asJava))
  }

  // given a name and a type T, builds the schema field for that type T. A schema field might itself contain
  // a nested record schema if T is a class. The provided annos are a wrapper around annotations.
  def fieldBuilder[T](name: String, annos: Seq[Anno])
                     (implicit toSchema: ToSchema[T]): Schema.Field = fieldBuilder(name, annos, toSchema())

  def fieldBuilder(name: String, annos: Seq[Anno], schema: Schema): Schema.Field = {
    val field = new Schema.Field(name, schema, doc(annos), null)
    aliases(annos).foreach(field.addAlias)
    addProps(annos, field.addProp)
    field
  }

  def recordBuilder[T](name: String, pack: String, fields: Seq[Schema.Field], annos: Seq[Anno]): Schema = {

    import scala.collection.JavaConverters._

    val schema = org.apache.avro.Schema.createRecord(name, doc(annos), pack, false)
    addProps(annos, schema.addProp)
    schema.setFields(fields.asJava)
    schema
  }
}

case class Anno(classname: String, values: Seq[String])
