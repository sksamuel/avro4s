package com.sksamuel.avro4s

import java.util
import java.util.UUID

import org.apache.avro.Schema
import shapeless.Lazy

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox

trait ToSchema[T] {
  def apply(): org.apache.avro.Schema
}

trait LowPriorityToSchema {
  implicit def apply[T: Manifest](implicit schemaFor: SchemaFor[T]): ToSchema[T] = new ToSchema[T] {
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

  implicit val ByteArrayToSchema: ToSchema[Array[Byte]] = new ToSchema[Array[Byte]] {
    def apply(): Schema = Schema.create(Schema.Type.BYTES)
  }

  implicit val DoubleToSchema: ToSchema[Double] = new ToSchema[Double] {
    def apply(): Schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit def EitherToSchema[A, B](implicit aSchema: ToSchema[A], bSchema: ToSchema[B]): ToSchema[Either[A, B]] = {
    new ToSchema[Either[A, B]] {
      def apply(): Schema = Schema.createUnion(util.Arrays.asList(aSchema.apply, bSchema.apply))
    }
  }

  implicit def JavaEnumToSchema[E <: Enum[_]](implicit tag: ClassTag[E]): ToSchema[E] = new ToSchema[E] {
    override def apply(): Schema = {
      val values = tag.runtimeClass.getEnumConstants.map(_.toString)
      Schema.createEnum(tag.runtimeClass.getSimpleName, null, tag.runtimeClass.getPackage.getName, values.toList.asJava)
    }
  }

  implicit val FloatToSchema: ToSchema[Float] = new ToSchema[Float] {
    def apply(): Schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit val IntToSchema: ToSchema[Int] = new ToSchema[Int] {
    def apply(): Schema = Schema.create(Schema.Type.INT)
  }

  implicit val LongToSchema: ToSchema[Long] = new ToSchema[Long] {
    def apply(): Schema = Schema.create(Schema.Type.LONG)
  }

  implicit val StringToSchema: ToSchema[String] = new ToSchema[String] {
    def apply(): Schema = Schema.create(Schema.Type.STRING)
  }

  implicit object UUIDToSchema extends ToSchema[java.util.UUID] {
    def apply(): Schema = Schema.create(Schema.Type.STRING)
  }

  implicit def MapToSchema[V](implicit valueToSchema: ToSchema[V]): ToSchema[Map[String, V]] = {
    new ToSchema[Map[String, V]] {
      def apply(): Schema = Schema.createMap(valueToSchema())
    }
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

  implicit def SeqToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Seq[S]] = new ToSchema[Seq[S]] {
    def apply(): Schema = Schema.createArray(subschema())
  }
}

@implicitNotFound("Cout not find implicit SchemaFor[${T}]")
trait SchemaFor[T] {
  def apply(): org.apache.avro.Schema
}

object SchemaFor {

  implicit def apply[T]: SchemaFor[T] = macro SchemaFor.applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: whitebox.Context): c.Expr[SchemaFor[T]] = {
    import c.universe._
    val tType = weakTypeOf[T]
    require(tType.typeSymbol.isClass, tType + " is not a class but is " + tType.typeSymbol.fullName)

    def annotations(sym: Symbol): Seq[c.Tree] = sym.annotations.map { a =>
      val name = a.tree.tpe.typeSymbol.fullName
      val args = a.tree.children.tail.map(_.toString.stripPrefix("\"").stripSuffix("\""))
      q"com.sksamuel.avro4s.Anno($name, $args)"
    }

    def fieldsForType(atype: c.universe.Type): List[c.universe.Symbol] = {
      atype.decls.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m.paramLists.head
      }.getOrElse(Nil)
    }

    val sealedTraitOrClass = tType.typeSymbol.isClass && tType.typeSymbol.asClass.isSealed

    val fieldSchemaPartTrees: Seq[Tree] = if (sealedTraitOrClass) {
      val internal = tType.typeSymbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
      val descendants = internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - tType.typeSymbol
      descendants.flatMap(x => fieldsForType(x.asType.toType)).groupBy(_.name.decodedName.toString).map { case (name, fs) =>
        val schemas = fs map { f =>
          val sig = f.typeSignature
          q"""{
               import com.sksamuel.avro4s.SchemaFor._
               import com.sksamuel.avro4s.ToSchema._
               com.sksamuel.avro4s.SchemaFor.schemaBuilder[$sig]
               }
           """
        }
        // if we have the same number of schemas as the number of descendants then it means every subclass
        // of the trait has that particular field, so we can make it non optional, otherwise it means at least
        // one subclass of the trait is missing the field, and so it must be marked optional
        val optional = schemas.size != descendants.size
        q""" com.sksamuel.avro4s.SchemaFor.unionBuilder($name, Set(..$schemas), $optional) """
      }.toSeq
    } else {

      val fields = tType.decls.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m.paramLists.head
      }.getOrElse(Nil)

      fields.map { f =>
        val name = f.name
        // the simple name of the field
        val decoded = name.decodedName.toString.trim
        // the full path of the field, eg a.b.c.Class.value
        val fieldPath = f.fullName
        val sig = f.typeSignature
        val annos = annotations(f)

        if (f.typeSignature.<:<(typeOf[scala.Enumeration#Value])) {

          //          val path = Paths.get("/home/sam/development/workspace/avro4s/debug")
          //
          //          val g = f.typeSignature.decls.collect {
          //            case m if m.fullName.contains("outerEnum") =>
          //              if (m.isMethod) {
          //                Files.write(path, (m.asMethod.returnType + "\n").getBytes, StandardOpenOption.APPEND)
          //                Files.write(path, (m.asMethod.returnType.typeSymbol + "\n").getBytes, StandardOpenOption.APPEND)
          //              }
          //          }.toList
          val enumClass = f.typeSignature.toString.stripSuffix(".Value")
          q"""{
            com.sksamuel.avro4s.SchemaFor.enumBuilder($decoded, $enumClass)
            }
           """

        } else {
          q"""{
            com.sksamuel.avro4s.SchemaFor.fieldBuilder[$sig]($decoded, Seq(..$annos))
            }
           """
        }
      }
    }

    // name of the actual class
    val name = tType.typeSymbol.name.decodedName.toString
    // name of the outer package, can't find a way to get this explicitly so hacking the full class name
    val pack = tType.typeSymbol.fullName.split('.').takeWhile(_.forall(c => !c.isUpper)).mkString(".")
    val annos = annotations(tType.typeSymbol)

    c.Expr[SchemaFor[T]](
      q"""
           new com.sksamuel.avro4s.SchemaFor[$tType] {
            def apply(): org.apache.avro.Schema = {
              com.sksamuel.avro4s.SchemaFor.recordBuilder[$tType](
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
  def schemaBuilder[T](implicit toSchema: Lazy[ToSchema[T]]): Schema = toSchema.value()

  def unionBuilder(name: String, schemas: Set[Schema], optional: Boolean): Schema.Field = {
    val sortedSchemas = (if (optional) schemas + Schema.create(Schema.Type.NULL) else schemas).toSeq.sortBy(_.getName)
    fieldBuilder(name, Nil, Schema.createUnion(sortedSchemas.asJava))
  }

  /**
    * Given a name and a type T, builds a enum scala by taking the prefix of the .Value class
    * and probing that for enum values
    */
  def enumBuilder(name: String, enumClassName: String): Schema.Field = {
    val enumClass = Class.forName(enumClassName)
    val values = enumClass.getMethod("values").invoke(null).asInstanceOf[scala.Enumeration#ValueSet].iterator.toList.map(_.toString)
    val schema = Schema.createEnum(enumClass.getSimpleName, null, enumClass.getPackage.getName, values.asJava)
    new Schema.Field(name, schema, null, null)
  }

  // given a name and a type T, builds the schema field for that type T. A schema field might itself contain
  // a nested record schema if T is a class. The provided annos are a wrapper around annotations.
  def fieldBuilder[T](name: String, annos: Seq[Anno])(implicit toSchema: Lazy[ToSchema[T]]): Schema.Field = {
    fieldBuilder(name, annos, toSchema.value.apply())
  }

  private def fieldBuilder(name: String, annos: Seq[Anno], schema: Schema): Schema.Field = {
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
