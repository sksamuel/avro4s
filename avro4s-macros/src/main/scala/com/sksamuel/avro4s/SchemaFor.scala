package com.sksamuel.avro4s

import java.time.{LocalDate, LocalDateTime}
import java.util
import org.apache.avro.Schema.Field
import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaBuilder}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{:+:, CNil, Coproduct, Generic, HList, Lazy}

import scala.annotation.{implicitNotFound, tailrec}
import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.math.BigDecimal.RoundingMode.{RoundingMode, UNNECESSARY}
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

case class ScaleAndPrecisionAndRoundingMode(scale: Int, precision: Int, roundingMode: RoundingMode)

object ToSchema extends LowPriorityToSchema {

  implicit object BooleanToSchema extends ToSchema[Boolean] {
    protected val schema = Schema.create(Schema.Type.BOOLEAN)
  }

  lazy val defaultScaleAndPrecisionAndRoundingMode = ScaleAndPrecisionAndRoundingMode(2, 8, UNNECESSARY)

  implicit def BigDecimalToSchema(implicit sp: ScaleAndPrecisionAndRoundingMode = defaultScaleAndPrecisionAndRoundingMode): ToSchema[BigDecimal] = new ToSchema[BigDecimal] {
    protected val schema = {
      val schema = Schema.create(Schema.Type.BYTES)
      LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(schema)
      schema
    }
  }

  implicit object ByteToSchema extends ToSchema[Byte] {
    protected val schema = Schema.create(Schema.Type.INT)
  }

  implicit object ShortToSchema extends ToSchema[Short] {
    protected val schema = Schema.create(Schema.Type.INT)
  }

  implicit object ByteArrayToSchema extends ToSchema[Array[Byte]] {
    protected val schema = Schema.create(Schema.Type.BYTES)
  }

  implicit object ByteSeqToSchema extends ToSchema[Seq[Byte]] {
    protected val schema = Schema.create(Schema.Type.BYTES)
  }

  implicit object DoubleToSchema extends ToSchema[Double] {
    protected val schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit def EitherToSchema[A, B](implicit aSchema: ToSchema[A], bSchema: ToSchema[B]): ToSchema[Either[A, B]] = {
    new ToSchema[Either[A, B]] {
      protected val schema = Schema.createUnion(util.Arrays.asList(aSchema(), bSchema()))
    }
  }

  implicit def JavaEnumToSchema[E <: Enum[_]](implicit tag: ClassTag[E]): ToSchema[E] = new ToSchema[E] {
    protected val schema = {
      val values = tag.runtimeClass.getEnumConstants.map(_.toString)
      Schema.createEnum(tag.runtimeClass.getSimpleName, null, tag.runtimeClass.getPackage.getName, values.toList.asJava)
    }
  }

  implicit def ScalaEnumToSchema[E <: scala.Enumeration#Value](implicit tag: TypeTag[E]): ToSchema[E] = new ToSchema[E] {
    val typeRef = tag.tpe match {
      case t@TypeRef(_, _, _) => t
    }

    val namespaceOpt = typeRef.pre.typeSymbol.annotations.collectFirst {
      case a if a.tree.tpe <:< typeOf[AvroNamespace] => a.tree.children.tail.collectFirst {
        case Literal(Constant(name: String)) => name
      }
    }.flatten

    protected val schema =
      SchemaFor.schemaForEnumClass(typeRef.pre.typeSymbol.asClass.fullName.stripSuffix(".Value"), namespaceOpt)
  }

  implicit object FloatToSchema extends ToSchema[Float] {
    protected val schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit object IntToSchema extends ToSchema[Int] {
    protected val schema = Schema.create(Schema.Type.INT)
  }

  implicit object LongToSchema extends ToSchema[Long] {
    protected val schema = Schema.create(Schema.Type.LONG)
  }

  implicit object StringToSchema extends ToSchema[String] {
    protected val schema = Schema.create(Schema.Type.STRING)
  }

  implicit object UUIDToSchema extends ToSchema[java.util.UUID] {
    protected val schema = {
      val schema = Schema.create(Schema.Type.STRING)
      LogicalTypes.uuid().addToSchema(schema)
      schema
    }
  }

  implicit object LocalDateToSchema extends ToSchema[LocalDate] {
    protected val schema = Schema.create(Schema.Type.STRING)
  }

  implicit object LocalDateTimeToSchema extends ToSchema[LocalDateTime] {
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
      protected val schema = Schema.createArray(subschema())
    }
  }

  implicit def IterableToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Iterable[S]] = {
    new ToSchema[Iterable[S]] {
      protected val schema = Schema.createArray(subschema())
    }
  }

  implicit def ListToSchema[S](implicit subschema: ToSchema[S]): ToSchema[List[S]] = {
    new ToSchema[List[S]] {
      protected val schema = Schema.createArray(subschema())
    }
  }

  implicit def SetToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Set[S]] = {
    new ToSchema[Set[S]] {
      protected val schema = Schema.createArray(subschema())
    }
  }

  implicit def VectorToSchema[S](implicit toschema: ToSchema[S]): ToSchema[Vector[S]] = {
    new ToSchema[Vector[S]] {
      protected val schema = Schema.createArray(toschema())
    }
  }

  implicit def SeqToSchema[S](implicit subschema: ToSchema[S]): ToSchema[Seq[S]] = new ToSchema[Seq[S]] {
    protected val schema = Schema.createArray(subschema())
  }

  implicit def Tuple2ToSchema[A, B](implicit a: SchemaFor[A], b: SchemaFor[B]) = new ToSchema[(A, B)] {
    override protected val schema: Schema = {
      val _1 = new Schema.Field("_1", a(), null, null: Object)
      val _2 = new Schema.Field("_2", b(), null, null: Object)
      Schema.createRecord("element", null, null, false, Seq(_1, _2).asJava)
    }
  }

  implicit def Tuple3ToSchema[A, B, C](implicit a: SchemaFor[A],
                                       b: SchemaFor[B],
                                       c: SchemaFor[C]) = new ToSchema[(A, B, C)] {
    override protected val schema: Schema = {
      val _1 = new Schema.Field("_1", a(), null, null: Object)
      val _2 = new Schema.Field("_2", b(), null, null: Object)
      val _3 = new Schema.Field("_3", c(), null, null: Object)
      Schema.createRecord("element", null, null, false, Seq(_1, _2, _3).asJava)
    }
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
  implicit def CoproductSchema[S, T <: Coproduct](implicit subschema: ToSchema[S], coproductSchema: ToSchema[T]): ToSchema[S :+: T] = new ToSchema[S :+: T] {
    protected val schema = createUnion(subschema(), coproductSchema())
  }

  // This ToSchema is used for sealed traits of objects
  implicit def genTraitObjectEnum[T, C <: Coproduct, L <: HList](implicit ct: ClassTag[T],
                                                                 tag: TypeTag[T],
                                                                 gen: Generic.Aux[T, C],
                                                                 objs: Reify.Aux[C, L],
                                                                 toList: ToList[L, T]): ToSchema[T] = new ToSchema[T] {
    protected val schema: Schema = {
      val tpe = weakTypeTag[T]
      val namespace = tpe.tpe.typeSymbol.annotations.map(_.toString)
        .find(_.startsWith("com.sksamuel.avro4s.AvroNamespace"))
        .map(_.stripPrefix("com.sksamuel.avro4s.AvroNamespace(\"").stripSuffix("\")"))
        .getOrElse(ct.runtimeClass.getPackage.getName)
      val name = ct.runtimeClass.getSimpleName
      val symbols = toList(objs()).map(_.toString).asJava
      Schema.createEnum(name, null, namespace, symbols)
    }
  }

  /**
    * Creates a new Avro Schema by combining the given schemas into a union.
    */
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

  implicit object ByteSchemaFor extends SchemaFor[Byte] {
    private val schema = SchemaBuilder.builder().intType()
    def apply(): org.apache.avro.Schema = schema
  }

  implicit object ShortSchemaFor extends SchemaFor[Short] {
    private val schema = SchemaBuilder.builder().intType()
    def apply(): org.apache.avro.Schema = schema
  }

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
      q"_root_.com.sksamuel.avro4s.Anno($name, $args)"
    }

    def genericNameSuffix(underlyingType: c.universe.Type): String = {
      lazy val isAnnotated = underlyingType.typeSymbol.annotations.exists { a =>
        a.tree.tpe.typeSymbol.fullName match {
          case "com.sksamuel.avro4s.AvroSpecificGeneric" =>
            val args = a.tree.children.tail.map(_.toString.stripPrefix("\"").stripSuffix("\""))
            args match {
              case head :: Nil => head.toBoolean
              case _ => false
            }
          case _ => false
        }
      }
      if (underlyingType.typeArgs.nonEmpty && isAnnotated) {
        underlyingType.typeArgs.map(_.typeSymbol.name.decodedName.toString).mkString("_", "_", "")
      } else {
        ""
      }
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

    // name of the actual class we are building
    val name = underlyingType.typeSymbol.name.decodedName.toString + genericNameSuffix(underlyingType)

    // the default namespace is just the package name
    val defaultNamespace = Stream.iterate(underlyingType.typeSymbol.owner)(_.owner).dropWhile(!_.isPackage).head.fullName

    // we read all annotations into quasi-quotable Anno dtos
    val annos = annotations(underlyingType.typeSymbol)

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

        val defswithsymbols = universe.asInstanceOf[Definitions with SymbolTable with StdNames]

        // this gets the method that generates the default value for this field
        // (if the field has a default value otherwise its a nosymbol)
        val defaultGetter = defswithsymbols.nme.defaultGetterName(defswithsymbols.nme.CONSTRUCTOR, index + 1)

        // this is a method symbol for the default getter if it exists
        val member = underlyingType.companion.member(TermName(defaultGetter.toString))

        // if the field is a param with a default value, then we know the getter method will be defined
        // and so we can use it to generate the default value
        if (f.isTerm && f.asTerm.isParamWithDefault && member.isMethod) {
          val moduleSym = underlyingType.typeSymbol.companion
          q"""{ _root_.com.sksamuel.avro4s.SchemaFor.fieldBuilder[$sig]($fieldName, Seq(..$annos), $moduleSym.$member, $defaultNamespace) }"""
        } else if (f.typeSignature.<:<(typeOf[scala.Enumeration#Value])) {
          val enumClass = f.typeSignature.toString.stripSuffix(".Value")
          q"""{_root_.com.sksamuel.avro4s.SchemaFor.enumBuilder($fieldName, $enumClass, Seq(..$annos))}"""
        } else {
          q"""{ _root_.com.sksamuel.avro4s.SchemaFor.fieldBuilder[$sig]($fieldName, Seq(..$annos), null, $defaultNamespace) }"""
        }
      }
    }

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
        new _root_.com.sksamuel.avro4s.SchemaFor[$tType] {
          private val schema: _root_.org.apache.avro.Schema = _root_.com.sksamuel.avro4s.SchemaFor.valueInvoker[$underlyingType]
          def apply(): _root_.org.apache.avro.Schema = schema
        }
      """
      )
    } else {
      fixedAnnotation match {
        case Some(AvroFixed(size)) =>
          val expr = c.Expr[SchemaFor[T]](
            q"""_root_.com.sksamuel.avro4s.SchemaFor.fixedSchemaFor[$tType]($name, Seq(..$annos), $defaultNamespace, $size)"""
          )
          expr
        case None =>
          c.Expr[SchemaFor[T]](
            q"""
            new _root_.com.sksamuel.avro4s.SchemaFor[$tType] {
              val (incompleteSchema: _root_.org.apache.avro.Schema, completeSchema: _root_.shapeless.Lazy[_root_.org.apache.avro.Schema]) = {
               _root_.com.sksamuel.avro4s.SchemaFor.recordBuilder[$tType](
                  $name,
                  $defaultNamespace,
               _root_.shapeless.Lazy {
                    val selfSchema = incompleteSchema
                    implicit val _: _root_.com.sksamuel.avro4s.ToSchema[$tType] = new _root_.com.sksamuel.avro4s.ToSchema[$tType] {
                      val schema: _root_.org.apache.avro.Schema = selfSchema
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

  // returns an optional avroname annotation present in the list of annotations
  def avroname(annos: Seq[Anno]): Option[String] = annotationsFor(classOf[AvroName], annos).headOption.flatMap(_.values.headOption)

  // returns an optional namespace if an annotation for AvroNamespace can be found
  def namespace(annos: Seq[Anno]): Option[String] = annotationsFor(classOf[AvroNamespace], annos).headOption.flatMap(_.values.headOption)

  def fixed(annos: Seq[Anno]): Option[Int] = annotationsFor(classOf[AvroFixed], annos).headOption.map(_.values.head.toInt)

  // extracts AvroProp(erties) from the list of annotations, if any, and applies each of them
  // in turn to the supplied receiver function.
  def addProps(annos: Seq[Anno], receiver: (String, String) => Unit): Unit = {
    annotationsFor(classOf[AvroProp], annos).map(_.values.toList).foreach {
      case key :: value :: Nil => receiver(key, value)
      case other => sys.error(s"Invalid annotation value for AvroProp $other")
    }
  }

  /**
    * Given a name and a type T, builds a enum scala by taking the prefix of the .Value class
    * and probing that for enum values
    */
  def enumBuilder(name: String, enumClassName: String, annos: Seq[Anno]): Schema.Field = {
    val schema = schemaForEnumClass(enumClassName, namespace(annos))
    new Schema.Field(name, schema, null: String, null: Object)
  }

  def schemaForEnumClass(enumClassName: String, namespaceOpt: Option[String]): Schema = {
    val enumClass = Class.forName(enumClassName)
    val values = enumClass.getMethod("values").invoke(null).asInstanceOf[scala.Enumeration#ValueSet].iterator.toList.map(_.toString)
    Schema.createEnum(enumClass.getSimpleName, null, namespaceOpt.getOrElse(enumClass.getPackage.getName), values.asJava)
  }

  /**
    * Builds an Avro Field for a field of type T in a Scala type.
    *
    * A schema field might map to a record schema type if the type T is itself a complex type.
    *
    * Requires an implicit ToSchema which is a typeclass that will generate a field for a given type T.
    * To add support for new types, it should be sufficient to bring into scope an implementation
    * of the ToSchema typeclass.
    *
    * @param name    the name of the underlying field in Scala.
    * @param annos   any annotations declared on the field
    * @param default if a default value has been declared using scala's default overloads
    *
    */
  def fieldBuilder[T](name: String, annos: Seq[Anno], default: Any, parentNamespace: String)
                     (implicit toSchema: Lazy[ToSchema[T]]): Schema.Field = {
    fieldBuilder(name, annos, toSchema.value.apply(), default, parentNamespace)
  }

  private def fieldBuilder(name: String,
                           annos: Seq[Anno],
                           implicitSchema: Schema, // the schema picked up via implicit resolution of ToSchema
                           default: Any,
                           parentNamespace: String): Schema.Field = {
    @tailrec
    def toDefaultValue(value: Any): Any = value match {
      case x: Int => x
      case x: Long => x
      case x: Boolean => x
      case x: Double => x
      case x: Seq[_] => x.asJava
      case x: Map[_, _] => x.asJava
      case Some(x) => toDefaultValue(x)
      case None => JsonProperties.NULL_VALUE
      case _ => value.toString
    }

    val defaultValue = if (default == null) null else toDefaultValue(default)
    val finalAvroName = avroname(annos).getOrElse(name)

    // if we have annotated with @AvroFixed then we override the type and change it to a Fixed schema
    // we need to take the namespace from the parent
    val schema = fixed(annos).map { size => Schema.createFixed(name, doc(annos), parentNamespace, size) }.getOrElse(implicitSchema)

    // we may have annotated our field with @AvroNamespace so this namespace should be applied
    // to any schemas we have generated for this field
    val schemaWithResolvedNamespace = namespace(annos).map(overrideNamespace(schema, _)).getOrElse(schema)

    val field = new Schema.Field(finalAvroName, schemaWithResolvedNamespace, doc(annos), defaultValue)
    aliases(annos).foreach(field.addAlias)
    addProps(annos, field.addProp)
    field
  }

  def recordBuilder[T](name: String,
                       parentNamespace: String,
                       fields: Lazy[Seq[Schema.Field]],
                       annos: Seq[Anno]): (Schema, Lazy[Schema]) = {

    import scala.collection.JavaConverters._

    val schema = org.apache.avro.Schema.createRecord(name, doc(annos), namespace(annos).getOrElse(parentNamespace), false)
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
