package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema
import org.codehaus.jackson.node.TextNode
import shapeless.Lazy

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.macros.whitebox

trait ToSchema[T] {
  protected val schema: Schema

  def apply(): Schema = schema
}

trait LowPriorityToSchema {
  implicit def apply[T: Manifest](implicit schemaFor: SchemaFor[T]): ToSchema[T] = new ToSchema[T] {
    protected val schema = schemaFor()
  }
}

object ToSchema extends LowPriorityToSchema {

  implicit val BooleanToSchema: ToSchema[Boolean] = new ToSchema[Boolean] {
    protected val schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit val BigDecimalToSchema: ToSchema[BigDecimal] = new ToSchema[BigDecimal] {
    protected val schema = Schema.create(Schema.Type.BYTES)
    schema.addProp("logicalType", "decimal")
    schema.addProp("scale", "2")
    schema.addProp("precision", "8")
  }

  implicit val ByteArrayToSchema: ToSchema[Array[Byte]] = new ToSchema[Array[Byte]] {
    protected val schema = Schema.create(Schema.Type.BYTES)
  }

  implicit val DoubleToSchema: ToSchema[Double] = new ToSchema[Double] {
    protected val schema =  Schema.create(Schema.Type.DOUBLE)
  }

  implicit def EitherToSchema[A, B](implicit aSchema: ToSchema[A], bSchema: ToSchema[B]): ToSchema[Either[A, B]] = {
    new ToSchema[Either[A, B]] {
      protected val schema = Schema.createUnion(util.Arrays.asList(aSchema.apply, bSchema.apply))
    }
  }

  implicit def JavaEnumToSchema[E <: Enum[_]](implicit tag: ClassTag[E]): ToSchema[E] = new ToSchema[E] {
    protected val schema = {
      val values = tag.runtimeClass.getEnumConstants.map(_.toString)
      Schema.createEnum(tag.runtimeClass.getSimpleName, null, tag.runtimeClass.getPackage.getName, values.toList.asJava)
    }
  }

  implicit val FloatToSchema: ToSchema[Float] = new ToSchema[Float] {
    protected val schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit val IntToSchema: ToSchema[Int] = new ToSchema[Int] {
    protected val schema = Schema.create(Schema.Type.INT)
  }

  implicit val LongToSchema: ToSchema[Long] = new ToSchema[Long] {
    protected val schema = Schema.create(Schema.Type.LONG)
  }

  implicit val StringToSchema: ToSchema[String] = new ToSchema[String] {
    protected val schema = Schema.create(Schema.Type.STRING)
  }

  implicit object UUIDToSchema extends ToSchema[java.util.UUID] {
    protected val schema = Schema.create(Schema.Type.STRING)
  }

  implicit def MapToSchema[V](implicit valueToSchema: ToSchema[V]): ToSchema[Map[String, V]] = {
    new ToSchema[Map[String, V]] {
      protected val schema = Schema.createMap(valueToSchema())
    }
  }

  implicit def OptionToSchema[T](implicit valueSchema: ToSchema[T]): ToSchema[Option[T]] = {
    new ToSchema[Option[T]] {
      protected val schema = Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), valueSchema.apply))
    }
  }

  implicit def ArrayToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Array[S]] = {
    new ToSchema[Array[S]] {
      protected val schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def IterableToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Iterable[S]] = {
    new ToSchema[Iterable[S]] {
      protected val schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def ListToSchema[S](implicit subschema: ToSchema[S]): ToSchema[List[S]] = {
    new ToSchema[List[S]] {
      protected val schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def SetToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Set[S]] = {
    new ToSchema[Set[S]] {
      protected val schema = Schema.createArray(subschema.apply)
    }
  }

  implicit def SeqToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Seq[S]] = new ToSchema[Seq[S]] {
    protected val schema = Schema.createArray(subschema())
  }
}

@implicitNotFound("Could not find implicit SchemaFor[${T}]")
trait SchemaFor[T] {
  def apply(): org.apache.avro.Schema
}

object SchemaFor {

  implicit def apply[T]: SchemaFor[T] = macro SchemaFor.applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: whitebox.Context): c.Expr[SchemaFor[T]] = {
    import c.universe
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

      val ctr = tType.decls.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m
      }.get

      val fields = ctr.paramLists.head

      fields.zipWithIndex.map { case (f, index) =>
        val name = f.name
        // the simple name of the field
        val fieldName = name.decodedName.toString.trim
        // the full path of the field, eg a.b.c.Class.value
        val fieldPath = f.fullName
        val sig = f.typeSignature
        val annos = annotations(f)

        // this gets the method that generates the default value for this field
        // (if the field has a default value otherwise its a nosymbol)
        val ds = universe.asInstanceOf[Definitions with SymbolTable with StdNames]
        val defaultGetter = ds.nme.defaultGetterName(ds.nme.CONSTRUCTOR, index + 1)
        val defaultGetterName = TermName(defaultGetter.toString)
        val member = tType.companion.member(defaultGetterName)

        if (f.isTerm && f.asTerm.isParamWithDefault && member.isMethod) {
          //val path = Paths.get("/home/sam/development/workspace/avro4s/debug")
          //path.toFile.createNewFile()
          //Files.write(path, (s"defaultMethodFor $fieldName $defaultGetterName\n").getBytes, StandardOpenOption.APPEND)
          //Files.write(path, (s"does method exist? $member1\n").getBytes, StandardOpenOption.APPEND)
          //Files.write(path, ("{ com.sksamuel.avro4s.SchemaFor.fieldBuilder[$sig]($fieldName, Seq(..$annos), null) }\n").getBytes, StandardOpenOption.APPEND)
          q"""{ com.sksamuel.avro4s.SchemaFor.fieldBuilder[$sig]($fieldName, Seq(..$annos), $member) }"""
        } else if (f.typeSignature.<:<(typeOf[scala.Enumeration#Value])) {
          val enumClass = f.typeSignature.toString.stripSuffix(".Value")
          q"""{ com.sksamuel.avro4s.SchemaFor.enumBuilder($fieldName, $enumClass) }
           """
        } else {
          q"""{ com.sksamuel.avro4s.SchemaFor.fieldBuilder[$sig]($fieldName, Seq(..$annos), null) }"""
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

           val schema : org.apache.avro.Schema = {
              com.sksamuel.avro4s.SchemaFor.recordBuilder[$tType](
                $name,
                $pack,
                Seq(..$fieldSchemaPartTrees),
                Seq(..$annos)
              )
            }
            def apply(): org.apache.avro.Schema = schema
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
    fieldBuilder(name, Nil, Schema.createUnion(sortedSchemas.asJava), null)
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
  def fieldBuilder[T](name: String, annos: Seq[Anno], default: Any)(implicit toSchema: Lazy[ToSchema[T]]): Schema.Field = {
    fieldBuilder(name, annos, toSchema.value.apply(), default)
  }

  private def fieldBuilder(name: String, annos: Seq[Anno], schema: Schema, default: Any): Schema.Field = {
    val defaultNode = if (default == null) null else new TextNode(default.toString)
    val field = new Schema.Field(name, schema, doc(annos), defaultNode)
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
