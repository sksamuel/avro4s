package com.sksamuel.avro4s

import java.util

import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaBuilder}
import shapeless.{:+:, CNil, Coproduct, Lazy}

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.language.experimental.macros
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

case class ScaleAndPrecision(scale: Int, precision: Int)

object ToSchema extends LowPriorityToSchema {

  implicit val BooleanToSchema: ToSchema[Boolean] = new ToSchema[Boolean] {
    protected val schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit def BigDecimalToSchema(implicit sp: ScaleAndPrecision = ScaleAndPrecision(2, 8)): ToSchema[BigDecimal] = new ToSchema[BigDecimal] {
    protected val schema = {
      val schema = Schema.create(Schema.Type.BYTES)
      LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(schema)
      schema
    }
  }

  implicit val ByteArrayToSchema: ToSchema[Array[Byte]] = new ToSchema[Array[Byte]] {
    protected val schema = Schema.create(Schema.Type.BYTES)
  }

  implicit val DoubleToSchema: ToSchema[Double] = new ToSchema[Double] {
    protected val schema = Schema.create(Schema.Type.DOUBLE)
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
      protected val schema = createUnion(Schema.create(Schema.Type.NULL), valueSchema())
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

  implicit def VectorToSchema[S](implicit toschema: ToSchema[S]): ToSchema[Vector[S]] = {
    new ToSchema[Vector[S]] {
      protected val schema = Schema.createArray(toschema.apply)
    }
  }

  implicit def SeqToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Seq[S]] = new ToSchema[Seq[S]] {
    protected val schema = Schema.createArray(subschema())
  }

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))
  // so here we define the schema for the base case of the recursion, C :+: CNil
  implicit def CoproductBaseSchema[S](implicit subschema: ToSchema[S]): ToSchema[S :+: CNil] = new ToSchema[S :+: CNil] {
    protected val schema = createUnion(subschema())
  }

  // And here we continue the recursion up.
  implicit def CoproductSchema[S,T <: Coproduct](implicit subschema: ToSchema[S], coproductSchema: ToSchema[T]): ToSchema[S :+: T] = new ToSchema[S :+: T] {
    protected val schema = createUnion(subschema(), coproductSchema())
  }

  private def createUnion(schemas: Schema*): Schema = {
    import scala.util.{Try, Success, Failure}

    // union schemas can't contain other union schemas as a direct
    // child, so whenever we create a union, we need to check if our
    // children are unions

    // if they are, we just merge them into the union we're creating

    def schemasOf(schema: Schema): Seq[Schema] = Try(schema.getTypes /* throws an error if we're not a union */) match {
      case Success(subschemas) => subschemas.asScala
      case Failure(_) => Seq(schema)
    }

    def moveNullToHead(schemas: Seq[Schema]) = {
      val (nulls, withoutNull) = schemas.partition(_.getType == Schema.Type.NULL)
      nulls.headOption.toSeq ++ withoutNull
    }

    val subschemas = schemas.flatMap(schemasOf)
    Schema.createUnion(moveNullToHead(subschemas).asJava)
  }
}

@implicitNotFound("Could not find implicit SchemaFor[${T}]")
trait SchemaFor[T] {
  def apply(): org.apache.avro.Schema
}

object SchemaFor {

  implicit object LongSchemaFor extends SchemaFor[Long] {
    private val schema = SchemaBuilder.builder().longType()
    def apply(): org.apache.avro.Schema = schema
  }

  implicit object IntSchemaFor extends SchemaFor[Int] {
    private val schema = SchemaBuilder.builder().intType()
    def apply(): org.apache.avro.Schema = schema
  }

  implicit object FloatSchemaFor extends SchemaFor[Float] {
    private val schema = SchemaBuilder.builder().floatType()
    def apply(): org.apache.avro.Schema = schema
  }

  implicit object DoubleSchemaFor extends SchemaFor[Double] {
    private val schema = SchemaBuilder.builder().doubleType()
    def apply(): org.apache.avro.Schema = schema
  }

  implicit object BooleanSchemaFor extends SchemaFor[Boolean] {
    private val schema = SchemaBuilder.builder().booleanType()
    def apply(): org.apache.avro.Schema = schema
  }

  implicit object StringSchemaFor extends SchemaFor[String] {
    private val schema = SchemaBuilder.builder().stringType()
    def apply(): org.apache.avro.Schema = schema
  }

  def fixedSchemaFor[T](name: String, annos: Seq[Anno], namespace: String, size: Int): SchemaFor[T] = {
    new SchemaFor[T] {
      private val schema = Schema.createFixed(name, doc(annos), namespace, size)

      override def apply(): Schema = schema
    }
  }

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

    lazy val fixedAnnotation: Option[AvroFixed] = tType.typeSymbol.annotations.collectFirst {
      case anno if anno.tree.tpe <:< c.weakTypeOf[AvroFixed] =>
        anno.tree.children.tail match {
          case Literal(Constant(size: Int)) :: Nil => AvroFixed(size)
        }
    }

    def fieldsForType(atype: c.universe.Type): List[c.universe.Symbol] = {
      atype.decls.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m.paramLists.head
      }.getOrElse(Nil)
    }

    val valueClass = tType.typeSymbol.isClass && tType.typeSymbol.asClass.isDerivedValueClass && fixedAnnotation.isEmpty
    val underlyingType = if (valueClass) {
      tType.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head.typeSignature
    } else {
      tType
    }
    val sealedTraitOrClass = underlyingType.typeSymbol.isClass && underlyingType.typeSymbol.asClass.isSealed

    val fieldSchemaPartTrees: Seq[Tree] = if (sealedTraitOrClass) {
      val internal = tType.typeSymbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
      val descendants = internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - tType.typeSymbol
      descendants.flatMap(x => fieldsForType(x.asType.toType)).groupBy(_.name.decodedName.toString).map { case (name, fs) =>
        val schemas = fs map { f =>
          val sig = f.typeSignature
          q"""com.sksamuel.avro4s.SchemaFor.schemaBuilder[$sig]"""
        }
        // if we have the same number of schemas as the number of descendants then it means every subclass
        // of the trait has that particular field, so we can make it non optional, otherwise it means at least
        // one subclass of the trait is missing the field, and so it must be marked optional
        val optional = schemas.size != descendants.size
        q""" com.sksamuel.avro4s.SchemaFor.unionBuilder($name, Set(..$schemas), $optional) """
      }.toSeq
    } else {

      val ctr = underlyingType.decls.collectFirst {
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
        val member = underlyingType.companion.member(defaultGetterName)

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
    val name = underlyingType.typeSymbol.name.decodedName.toString
    val pack = Stream.iterate(underlyingType.typeSymbol.owner)(_.owner).dropWhile(!_.isPackage).head.fullName
    val annos = annotations(underlyingType.typeSymbol)

    // we create an explicit ToSchema[T] in the scope of any
    // fieldBuilder calls, containing the incomplete schema

    // this is a higher priority implicit than
    // LowPriorityToSchema.apply(SchemaFor[T]): ToSchema[T], so it
    // avoids the use of the SchemaFor[T] we're in the middle of generating

    // that's a good thing, since that depends on completeSchema,
    // which is a Lazy value that ... depends on the fields we're
    // generating in those fieldBuilder calls
    if (valueClass) {
      c.Expr[SchemaFor[T]](
        q"""
        new com.sksamuel.avro4s.SchemaFor[$tType] {
          private val schema: org.apache.avro.Schema = com.sksamuel.avro4s.SchemaFor.valueInvoker[$underlyingType]
          def apply(): org.apache.avro.Schema = schema
        }
      """
      )
    } else {
      fixedAnnotation match {
        case Some(AvroFixed(size)) =>
          val expr = c.Expr[SchemaFor[T]](
            q"""
              com.sksamuel.avro4s.SchemaFor.fixedSchemaFor[$tType]($name, Seq(..$annos), $pack, $size)
            """
          )
          expr
        case None =>
          c.Expr[SchemaFor[T]](
            q"""
            new com.sksamuel.avro4s.SchemaFor[$tType] {
              val (incompleteSchema: org.apache.avro.Schema, completeSchema: shapeless.Lazy[org.apache.avro.Schema]) = {
                com.sksamuel.avro4s.SchemaFor.recordBuilder[$tType](
                  $name,
                  $pack,
                  shapeless.Lazy {
                    val selfSchema = incompleteSchema
                    implicit val selfToSchema: com.sksamuel.avro4s.ToSchema[$tType] = new com.sksamuel.avro4s.ToSchema[$tType] {
                      val schema: org.apache.avro.Schema = selfSchema
                    }
                    Seq(..$fieldSchemaPartTrees)
                  },
                  Seq(..$annos)
                )
              }

              def apply(): org.apache.avro.Schema = completeSchema.value
            }
          """
          )
      }
    }
  }

  def valueInvoker[T](implicit to: ToSchema[T]): org.apache.avro.Schema = to()

  def annotationsFor(klass: Class[_], annos: Seq[Anno]): Seq[Anno] = annos.filter(_.classname == klass.getName)

  def doc(annos: Seq[Anno]): String = {
    annos.find(_.classname == classOf[AvroDoc].getName).flatMap(_.values.headOption).orNull
  }

  // returns any aliases present in the list of annotations
  def aliases(annos: Seq[Anno]): Seq[String] = annotationsFor(classOf[AvroAlias], annos).flatMap(_.values.headOption)

  def fixed(annos: Seq[Anno]): Option[Int] = annotationsFor(classOf[AvroFixed], annos).headOption.map(_.values.head.toInt)

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
    new Schema.Field(name, schema, null: String, null: Object)
  }

  // given a name and a type T, builds the schema field for that type T. A schema field might itself contain
  // a nested record schema if T is a class. The provided annos are a wrapper around annotations.
  def fieldBuilder[T](name: String, annos: Seq[Anno], default: Any)(implicit toSchema: Lazy[ToSchema[T]]): Schema.Field = {
    fieldBuilder(name, annos, toSchema.value.apply(), default)
  }

  private def fieldBuilder(name: String, annos: Seq[Anno], schema: Schema, default: Any): Schema.Field = {
    def toDefaultValue(value: Any): Any = value match {
      case x: Int => x
      case x: Long => x
      case x: Boolean => x
      case x: Double => x
      case x: Seq[_] => x.asJava
      case x: Map[_, _] => x.asJava
      case Some(x) => x
      case None => JsonProperties.NULL_VALUE
      case _ => value.toString
    }

    val defaultValue = if (default == null) null else toDefaultValue(default)

    val field = new Schema.Field(name, schema, doc(annos), defaultValue)
    aliases(annos).foreach(field.addAlias)
    addProps(annos, field.addProp)
    field
  }

  def recordBuilder[T](name: String, pack: String, fields: Lazy[Seq[Schema.Field]], annos: Seq[Anno]): (Schema, Lazy[Schema]) = {

    import scala.collection.JavaConverters._

    val schema = org.apache.avro.Schema.createRecord(name, doc(annos), pack, false)
    // In recursive fields, the definition of the field depends on the
    // schema, but the definition of the schema depends on the
    // field. Furthermore, Schema and Field are java classes, strict
    // in their fields.

    // Thankfully, they're mutable!

    // So we can still manage to tie the knot by providing a strict
    // schema (to be used in field definitions) and a lazy schema
    // (with all the fields, only defined once the fields have been
    // defined).

    val fullSchema = fields.map { fields =>
      schema.setFields(fields.asJava)
      addProps(annos, schema.addProp(_, _: Any))
      schema
    }
    (schema, fullSchema)
  }
}
