package com.sksamuel.avro4s

import java.time.LocalDate
import java.util

import org.apache.avro.Schema.Field
import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaBuilder}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{:+:, CNil, Coproduct, Generic, HList, Lazy}

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe._

trait ToSchema[T] {
  protected val schema: Schema
  def apply(): Schema = schema
}

trait LowPriorityToSchema {

  implicit def genCoproduct[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
                                               coproductSchema: ToSchema[C]): ToSchema[T] = new ToSchema[T] {
    protected val schema: Schema = coproductSchema()
  }

  implicit def apply[T: Manifest](implicit schemaFor: SchemaFor[T]): ToSchema[T] = new ToSchema[T] {
    protected val schema = schemaFor()
  }
}

case class ScaleAndPrecision(scale: Int, precision: Int)

object ToSchema extends LowPriorityToSchema {

  implicit val BooleanToSchema: ToSchema[Boolean] = new ToSchema[Boolean] {
    protected val schema = Schema.create(Schema.Type.BOOLEAN)
  }

  lazy val defaultScaleAndPrecision = ScaleAndPrecision(2, 8)

  implicit def BigDecimalToSchema(implicit sp: ScaleAndPrecision = defaultScaleAndPrecision): ToSchema[BigDecimal] = new ToSchema[BigDecimal] {
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

  implicit def ScalaEnumToSchema[E <: scala.Enumeration#Value](implicit tag: TypeTag[E]): ToSchema[E] = new ToSchema[E] {
    val typeRef = tag.tpe match { case t @ TypeRef(_, _, _) => t}

    protected val schema = SchemaFor.schemaForEnumClass(typeRef.pre.typeSymbol.asClass.fullName.stripSuffix(".Value"))
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

  implicit object LocalDateToSchema extends ToSchema[LocalDate] {
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

  implicit def genTraitObjectEnum[T, C <: Coproduct, L <: HList](implicit ct: ClassTag[T],
                                                                 gen: Generic.Aux[T, C],
                                                                 objs: Reify.Aux[C, L],
                                                                 toList: ToList[L, T]): ToSchema[T] = new ToSchema[T] {
    protected val schema: Schema = {
      val name = ct.runtimeClass.getSimpleName
      val namespace = ct.runtimeClass.getPackage.getName
      val symbols = toList(objs()).map(_.toString).asJava
      Schema.createEnum(name, null, namespace, symbols)
    }
  }

  private def createUnion(schemas: Schema*): Schema = {
    import scala.util.{Failure, Success, Try}

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
    val helper = TypeHelper(c)
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

    val valueClass = tType.typeSymbol.isClass && tType.typeSymbol.asClass.isDerivedValueClass && fixedAnnotation.isEmpty
    val underlyingType = if (valueClass) {
      tType.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head.typeSignature
    } else {
      tType
    }
    val sealedTraitOrClass = underlyingType.typeSymbol.isClass && underlyingType.typeSymbol.asClass.isSealed

    val fieldSchemaPartTrees: Seq[Tree] = if (sealedTraitOrClass) {
      c.abort(c.prefix.tree.pos, "Sealed traits/classes should be handled by coproduct generic!")
    } else {

      val fields = helper.fieldsOf(underlyingType)

      fields.zipWithIndex.map { case ((f, sig), index) =>
        val name = f.name
        // the simple name of the field
        val fieldName = name.decodedName.toString.trim
        // the full path of the field, eg a.b.c.Class.value
        val fieldPath = f.fullName
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
                    implicit val _: com.sksamuel.avro4s.ToSchema[$tType] = new com.sksamuel.avro4s.ToSchema[$tType] {
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

  // returns an optioanl avroname annotation present in the list of annotations
  def avroname(annos: Seq[Anno]): Option[String] = annotationsFor(classOf[AvroName], annos).headOption.flatMap(_.values.headOption)

  def namespace(annos: Seq[Anno]): Option[String] = annotationsFor(classOf[AvroNamespace], annos).headOption.flatMap(_.values.headOption)

  def fixed(annos: Seq[Anno]): Option[Int] = annotationsFor(classOf[AvroFixed], annos).headOption.map(_.values.head.toInt)

  def addProps(annos: Seq[Anno], f: (String, String) => Unit): Unit = {
    annotationsFor(classOf[AvroProp], annos).map(_.values.toList).foreach {
      case key :: value :: Nil => f(key, value)
      case other => sys.error("Invalid annotation value " + other)
    }
  }

  /**
    * Given a name and a type T, builds a enum scala by taking the prefix of the .Value class
    * and probing that for enum values
    */
  def enumBuilder(name: String, enumClassName: String): Schema.Field = {
    val schema = schemaForEnumClass(enumClassName)
    new Schema.Field(name, schema, null: String, null: Object)
  }

  def schemaForEnumClass(enumClassName: String): Schema = {
    val enumClass = Class.forName(enumClassName)
    val values = enumClass.getMethod("values").invoke(null).asInstanceOf[scala.Enumeration#ValueSet].iterator.toList.map(_.toString)
    Schema.createEnum(enumClass.getSimpleName, null, enumClass.getPackage.getName, values.asJava)
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
    val finalAvroName = avroname(annos).getOrElse(name)
    val maybeOverrideNamespace = namespace(annos).map(overrideNamespace(schema, _)).getOrElse(schema)

    val field = new Schema.Field(finalAvroName, maybeOverrideNamespace, doc(annos), defaultValue)
    aliases(annos).foreach(field.addAlias)
    addProps(annos, field.addProp)
    field
  }

  def recordBuilder[T](name: String, pack: String, fields: Lazy[Seq[Schema.Field]], annos: Seq[Anno]): (Schema, Lazy[Schema]) = {

    import scala.collection.JavaConverters._

    val maybeNamespace = namespace(annos)
    val schema = org.apache.avro.Schema.createRecord(name, doc(annos), maybeNamespace.getOrElse(pack), false)
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

  private def overrideNamespace(schema: Schema, namespace: String): Schema =
    schema.getType match {
      case Schema.Type.RECORD =>
        val fields = schema.getFields.asScala.map(field =>
          new Field(field.name(), overrideNamespace(field.schema(), namespace), field.doc, field.defaultVal, field.order))
        Schema.createRecord(schema.getName, schema.getDoc, namespace, schema.isError, fields.asJava)
      case Schema.Type.UNION => Schema.createUnion(schema.getTypes.asScala.map(overrideNamespace(_, namespace)).asJava)
      case Schema.Type.ENUM => Schema.createEnum(schema.getName, schema.getDoc, namespace, schema.getEnumSymbols)
      case Schema.Type.FIXED => Schema.createFixed(schema.getName, schema.getDoc, namespace, schema.getFixedSize)
      case Schema.Type.MAP => Schema.createMap(overrideNamespace(schema.getValueType, namespace))
      case Schema.Type.ARRAY => Schema.createArray(overrideNamespace(schema.getElementType, namespace))
      case _ => schema
    }
}
