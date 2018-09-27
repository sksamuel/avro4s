package com.sksamuel.avro4s.internal

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import com.sksamuel.avro4s.{DefaultNamingStrategy, NamingStrategy}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{:+:, CNil, Coproduct, Generic, HList}

import scala.language.experimental.macros
import scala.math.BigDecimal.RoundingMode.{RoundingMode, UNNECESSARY}
import scala.reflect.ClassTag
import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe._

/**
  * A [[SchemaFor]] generates an Avro [[Schema]] for a Scala or Java type.
  *
  * For example, a String SchemaFor could return an instance of Schema.Type.STRING
  * or Schema.Type.FIXED depending on the type required for Strings.
  */
trait SchemaFor[T] extends Serializable {
  def schema: Schema
}

trait LowPrioritySchemaFor {

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))
  // so here we define the schema for the base case of the recursion, C :+: CNil
  implicit def coproductBaseSchema[S](implicit basefor: SchemaFor[S]): SchemaFor[S :+: CNil] = new SchemaFor[S :+: CNil] {

    import scala.collection.JavaConverters._

    val base = basefor.schema
    val schemas = scala.util.Try(base.getTypes.asScala).getOrElse(Seq(base))
    override val schema = Schema.createUnion(schemas: _*)
  }

  // And here we continue the recursion up.
  implicit def coproductSchema[S, T <: Coproduct](implicit basefor: SchemaFor[S], coproductFor: SchemaFor[T]): SchemaFor[S :+: T] = new SchemaFor[S :+: T] {
    val base = basefor.schema
    val coproduct = coproductFor.schema
    override val schema: Schema = SchemaFor.createSafeUnion(base, coproduct)
  }

  implicit def genCoproduct[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
                                               coproductFor: SchemaFor[C]): SchemaFor[T] = new SchemaFor[T] {
    override def schema: Schema = coproductFor.schema
  }
}

case class ScalePrecisionRoundingMode(scale: Int, precision: Int, roundingMode: RoundingMode)

object ScalePrecisionRoundingMode {
  implicit val default = ScalePrecisionRoundingMode(2, 8, UNNECESSARY)
}

object SchemaFor extends LowPrioritySchemaFor {

  import scala.collection.JavaConverters._

  def apply[T](implicit schemaFor: SchemaFor[T]): SchemaFor[T] = schemaFor

  implicit def applyMacro[T]: SchemaFor[T] = macro applyMacroImpl[T]

  def applyMacroImpl[T: c.WeakTypeTag](c: whitebox.Context): c.Expr[SchemaFor[T]] = {

    import c.universe
    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeOf[T]
    val fullName = tpe.typeSymbol.fullName
    val packageName = reflect.packageName(tpe)
    val isValueClass = reflect.isValueClass(tpe)
    val isSealed = reflect.isSealed(tpe)

    // eg, "Foo" for x.y.Foo
    val simpleName = tpe.typeSymbol.name.decodedName.toString

    // we can only encode classes in this macro
    if (!tpe.typeSymbol.isClass) {
      c.abort(c.prefix.tree.pos, s"$fullName is not a class")
    } else if (isSealed) {
      c.abort(c.prefix.tree.pos, s"$fullName is sealed: Sealed traits/classes should be handled by coproduct generic")
    } else {
      val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((f, fieldTpe), index) =>

        // the simple name of the field like "x"
        val fieldName = f.name.decodedName.toString.trim

        // if the field is a value type, we should include annotations defined on the value type as well
        val annos = if (reflect.isValueClass(fieldTpe)) {
          reflect.annotationsqq(f) ++ reflect.annotationsqq(fieldTpe.typeSymbol)
        } else {
          reflect.annotationsqq(f)
        }

        val defswithsymbols = universe.asInstanceOf[Definitions with SymbolTable with StdNames]

        // this gets the method that generates the default value for this field
        // (if the field has a default value otherwise its a nosymbol)
        val defaultGetterName = defswithsymbols.nme.defaultGetterName(defswithsymbols.nme.CONSTRUCTOR, index + 1)

        // this is a method symbol for the default getter if it exists
        val defaultGetterMethod = tpe.companion.member(TermName(defaultGetterName.toString))

        // if the field is a param with a default value, then we know the getter method will be defined
        // and so we can use it to generate the default value
        if (f.isTerm && f.asTerm.isParamWithDefault && defaultGetterMethod.isMethod) {
          //val method = reflect.defaultGetter(tType, index + 1)
          // the default method is defined on the companion object
          val moduleSym = tpe.typeSymbol.companion
          q"""{ _root_.com.sksamuel.avro4s.internal.SchemaFor.schemaField[$fieldTpe]($fieldName, $packageName, Seq(..$annos), $moduleSym.$defaultGetterMethod) }"""
        } else {
          q"""{ _root_.com.sksamuel.avro4s.internal.SchemaFor.schemaField[$fieldTpe]($fieldName, $packageName, Seq(..$annos), null) }"""
        }
      }

      // qualified name of the class we are building
      // todo encode generics:: + genericNameSuffix(underlyingType)
      //val className = tpe.typeSymbol.fullName.toString

      // these are annotations on the class itself
      val annos = reflect.annotationsqq(tpe.typeSymbol)

      c.Expr[SchemaFor[T]](
        q"""
        new _root_.com.sksamuel.avro4s.internal.SchemaFor[$tpe] {
          private val _schema = _root_.com.sksamuel.avro4s.internal.SchemaFor.buildSchema($simpleName, $packageName, Seq(..$fields), Seq(..$annos), $isValueClass)
          override def schema: _root_.org.apache.avro.Schema = _schema
        }
      """)
    }
  }

  /**
    * Builds an Avro [[Schema.Field]] with the field's [[Schema]] provided by an
    * implicit instance of [[SchemaFor]]. There must be a instance of this typeclass
    * in scope for any type we want to support in avro4s.
    *
    * Users can add their own mappings for types by implementing [[SchemaFor]] returning
    * the appropriate Avro schema.
    */
  def schemaField[B](name: String, packageName: String, annos: Seq[Anno], default: Any)
                    (implicit schemaFor: SchemaFor[B], namingStrategy: NamingStrategy = DefaultNamingStrategy): Schema.Field = {

    val extractor = new AnnotationExtractors(annos)
    val namespace = extractor.namespace.getOrElse(packageName)
    val doc = extractor.doc.orNull
    val aliases = extractor.aliases
    val props = extractor.props

    // the name could have been overriden with @AvroName, and then must be encoded with the naming strategy
    val resolvedName = extractor.name.fold(namingStrategy.to(name))(namingStrategy.to)

    // the default may be a scala type that avro doesn't understand, so we must turn it into a java type
    val resolvedDefault = resolveDefault(default)

    // if we have annotated with @AvroFixed then we override the type and change it to a Fixed schema
    // if someone puts @AvroFixed on a complex type, it makes no sense, but that's their cross to bear
    val schema = extractor.fixed.fold(schemaFor.schema) { size =>
      SchemaBuilder.fixed(name).doc(doc).namespace(namespace).size(size)
    }

    // for a union the type that has a default must be first
    val schemaWithOrderedUnion = if (schema.getType == Schema.Type.UNION && resolvedDefault != null) {
      moveDefaultToHead(schema, resolvedDefault)
    } else schema

    // the field can override the namespace if the Namespace annotation is present on the field
    // we may have annotated our field with @AvroNamespace so this namespace should be applied
    // to any schemas we have generated for this field
    val schemaWithResolvedNamespace = extractor.namespace.map(overrideNamespace(schemaWithOrderedUnion, _)).getOrElse(schemaWithOrderedUnion)

    val field = new Schema.Field(resolvedName, schemaWithResolvedNamespace, doc, resolvedDefault: AnyRef)
    props.foreach { case (k, v) => field.addProp(k, v: AnyRef) }
    aliases.foreach(field.addAlias)
    field
  }

  def buildSchema(name: String,
                  packageName: String,
                  fields: Seq[Schema.Field],
                  annotations: Seq[Anno],
                  valueType: Boolean): Schema = {

    import scala.collection.JavaConverters._

    val extractor = new AnnotationExtractors(annotations)
    val doc = extractor.doc.orNull
    val namespace = extractor.namespace.getOrElse(packageName)
    val aliases = extractor.aliases
    val props = extractor.props

    // if the class is a value type, then we need to use the schema for the single field of the type
    // if we have a value type AND @AvroFixed is present, then we return a schema of type fixed
    if (valueType) {
      val field = fields.head
      extractor.fixed.fold(field.schema) { size =>
        val builder = SchemaBuilder.fixed(name).doc(doc).namespace(namespace).aliases(aliases: _*)
        props.foreach { case (k, v) => builder.prop(k, v) }
        builder.size(size)
      }
    } else {
      val record = Schema.createRecord(name, doc, namespace, false)
      aliases.foreach(record.addAlias)
      props.foreach { case (k, v) => record.addProp(k: String, v: AnyRef) }
      record.setFields(fields.asJava)
      record
    }
  }

  // accepts a built avro schema, and overrides the namespace with the given namespace
  // this method just just makes a copy of the existing schema, setting the new namespace
  private def overrideNamespace(schema: Schema, namespace: String): Schema = {
    schema.getType match {
      case Schema.Type.RECORD =>
        val fields = schema.getFields.asScala.map { field =>
          new Schema.Field(field.name(), overrideNamespace(field.schema(), namespace), field.doc, field.defaultVal, field.order)
        }
        val copy = Schema.createRecord(schema.getName, schema.getDoc, namespace, schema.isError, fields.asJava)
        schema.getAliases.asScala.foreach(copy.addAlias)
        schema.getObjectProps.asScala.foreach { case (k, v) => copy.addProp(k, v) }
        copy
      case Schema.Type.UNION => Schema.createUnion(schema.getTypes.asScala.map(overrideNamespace(_, namespace)).asJava)
      case Schema.Type.ENUM => Schema.createEnum(schema.getName, schema.getDoc, namespace, schema.getEnumSymbols)
      case Schema.Type.FIXED => Schema.createFixed(schema.getName, schema.getDoc, namespace, schema.getFixedSize)
      case Schema.Type.MAP => Schema.createMap(overrideNamespace(schema.getValueType, namespace))
      case Schema.Type.ARRAY => Schema.createArray(overrideNamespace(schema.getElementType, namespace))
      case _ => schema
    }
  }

  // returns a default value that is compatible with the datatype
  // for example, we might define a case class with a UUID field with a default value
  // of UUID.randomUUID(), but in avro UUIDs are logical types. Therefore the default
  // values must be converted into a base type avro understands.
  // another example would be `name: Option[String] = Some("abc")`, we can't use
  // the Some as the default, the inner value needs to be extracted
  private def resolveDefault(default: Any): AnyRef = {
    println(s"Resolving default = $default")
    default match {
      case null => null
      case UUIDType => default.toString
      case bd: BigDecimal => java.lang.Double.valueOf(bd.underlying.doubleValue)
      case Some(value) => resolveDefault(value)
      case b: Boolean => java.lang.Boolean.valueOf(b)
      case i: Int => java.lang.Integer.valueOf(i)
      case d: Double => java.lang.Double.valueOf(d)
      case s: Short => java.lang.Short.valueOf(s)
      case l: Long => java.lang.Long.valueOf(l)
      case f: Float => java.lang.Float.valueOf(f)
      case other => other.toString
    }
    //    dataType match {
    //
    //      case StringType => default.toString

    //      case NullableType(elementType) => default match {
    //        case Some(value) => resolveDefault(value, elementType)
    //        case None => null
    //      }
    //      case _ => default.toString
    //    }
  }

  def moveDefaultToHead(schema: Schema, default: Any): Schema = {
    require(schema.getType == Schema.Type.UNION)
    val defaultType = default match {
      case _: String => Schema.Type.STRING
      case _: Long => Schema.Type.LONG
      case _: Int => Schema.Type.INT
      case _: Boolean => Schema.Type.BOOLEAN
      case _: Float => Schema.Type.FLOAT
      case _: Double => Schema.Type.DOUBLE
      case other => other
    }
    val (first, rest) = schema.getTypes.asScala.partition(_.getType == defaultType)
    val result = Schema.createUnion(first.headOption.toSeq ++ rest: _*)
    schema.getObjectProps.asScala.foreach { case (k, v) => result.addProp(k, v) }
    result
  }

  def moveNullToHead(schema: Schema): Schema = {
    val (nulls, rest) = schema.getTypes.asScala.partition(_.getType == Schema.Type.NULL)
    val result = Schema.createUnion(nulls.headOption.toSeq ++ rest: _*)
    schema.getAliases.asScala.foreach(result.addAlias)
    schema.getObjectProps.asScala.foreach { case (k, v) => result.addProp(k, v) }
    result
  }

  // creates a union schema type, with nested unions extracted, and duplicate nulls stripped
  // union schemas can't contain other union schemas as a direct
  // child, so whenever we create a union, we need to check if our
  // children are unions and flatten
  def createSafeUnion(schemas: Schema*): Schema = {
    val flattened = schemas.flatMap(schema => scala.util.Try(schema.getTypes.asScala).getOrElse(Seq(schema)))
    val (nulls, rest) = flattened.partition(_.getType == Schema.Type.NULL)
    Schema.createUnion(nulls.headOption.toSeq ++ rest: _*)
  }

  implicit object ByteSchemaFor extends SchemaFor[Byte] {
    override val schema: Schema = SchemaBuilder.builder().intType()
  }

  implicit object StringSchemaFor extends SchemaFor[String] {
    override val schema: Schema = SchemaBuilder.builder().stringType()
  }

  implicit object LongFor extends SchemaFor[Long] {
    override val schema: Schema = SchemaBuilder.builder().longType()
  }

  implicit object IntFor extends SchemaFor[Int] {
    override val schema: Schema = SchemaBuilder.builder().intType()
  }

  implicit object DoubleFor extends SchemaFor[Double] {
    override val schema: Schema = SchemaBuilder.builder().doubleType()
  }

  implicit object FloatFor extends SchemaFor[Float] {
    override val schema: Schema = SchemaBuilder.builder().floatType()
  }

  implicit object BooleanFor extends SchemaFor[Boolean] {
    override val schema: Schema = SchemaBuilder.builder().booleanType()
  }

  implicit object ShortSchemaFor extends SchemaFor[Short] {
    override val schema: Schema = SchemaBuilder.builder().intType()
  }

  implicit object UUIDSchemaFor extends SchemaFor[UUID] {
    override val schema: Schema = SchemaBuilder.builder().stringType()
    LogicalTypes.uuid().addToSchema(schema)
  }

  implicit object ByteArraySchemaFor extends SchemaFor[Array[Byte]] {
    override val schema: Schema = SchemaBuilder.builder().bytesType()
  }

  implicit object ByteSeqSchemaFor extends SchemaFor[Seq[Byte]] {
    override val schema: Schema = SchemaBuilder.builder().bytesType()
  }

  implicit def bigDecimalFor(implicit sp: ScalePrecisionRoundingMode = ScalePrecisionRoundingMode.default): SchemaFor[BigDecimal] = new SchemaFor[BigDecimal] {
    override val schema = Schema.create(Schema.Type.BYTES)
    LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(schema)
  }

  implicit def eitherSchemaFor[A, B](implicit leftFor: SchemaFor[A], rightFor: SchemaFor[B]): SchemaFor[Either[A, B]] = {
    new SchemaFor[Either[A, B]] {
      override val schema: Schema = Schema.createUnion(leftFor.schema, rightFor.schema)
    }
  }

  implicit def optionSchemaFor[T](implicit elementType: SchemaFor[T]): SchemaFor[Option[T]] = {
    new SchemaFor[Option[T]] {
      override val schema: Schema = {
        val elementSchema = elementType.schema
        val nullSchema = SchemaBuilder.builder().nullType()
        SchemaFor.createSafeUnion(elementSchema, nullSchema)
      }
    }
  }

  implicit def ArraySchemaFor[S](implicit elementType: SchemaFor[S]): SchemaFor[Array[S]] = {
    new SchemaFor[Array[S]] {
      override val schema: Schema = Schema.createArray(elementType.schema)
    }
  }

  implicit def IterableFor[S](implicit elementType: SchemaFor[S]): SchemaFor[Iterable[S]] = {
    new SchemaFor[Iterable[S]] {
      override val schema: Schema = Schema.createArray(elementType.schema)
    }
  }

  implicit def ListFor[S](implicit elementType: SchemaFor[S]): SchemaFor[List[S]] = {
    new SchemaFor[List[S]] {
      override val schema: Schema = Schema.createArray(elementType.schema)
    }
  }

  implicit def SetFor[S](implicit elementType: SchemaFor[S]): SchemaFor[Set[S]] = {
    new SchemaFor[Set[S]] {
      override val schema: Schema = Schema.createArray(elementType.schema)
    }
  }

  implicit def VectorFor[S](implicit elementType: SchemaFor[S]): SchemaFor[Vector[S]] = {
    new SchemaFor[Vector[S]] {
      override val schema: Schema = Schema.createArray(elementType.schema)
    }
  }

  implicit def SeqFor[S](implicit elementType: SchemaFor[S]): SchemaFor[Seq[S]] = new SchemaFor[Seq[S]] {
    override val schema: Schema = Schema.createArray(elementType.schema)
  }

  implicit def Tuple2ToSchema[A, B](implicit a: SchemaFor[A], b: SchemaFor[B]): SchemaFor[(A, B)] = new SchemaFor[(A, B)] {
    override val schema: Schema =
      SchemaBuilder.record("Tuple2").namespace("scala").doc(null)
        .fields()
        .name("_1").`type`(a.schema).noDefault()
        .name("_2").`type`(b.schema).noDefault()
        .endRecord()
  }

  implicit def Tuple3ToSchema[A, B, C](implicit
                                       a: SchemaFor[A],
                                       b: SchemaFor[B],
                                       c: SchemaFor[C]): SchemaFor[(A, B, C)] = new SchemaFor[(A, B, C)] {
    override val schema: Schema =
      SchemaBuilder.record("Tuple3").namespace("scala").doc(null)
        .fields()
        .name("_1").`type`(a.schema).noDefault()
        .name("_2").`type`(b.schema).noDefault()
        .name("_3").`type`(c.schema).noDefault()
        .endRecord()
  }

  implicit def MapFor[V](implicit valueType: SchemaFor[V]): SchemaFor[Map[String, V]] = {
    new SchemaFor[Map[String, V]] {
      override def schema: Schema = SchemaBuilder.map().values(valueType.schema)
    }
  }

  implicit object TimestampFor extends SchemaFor[Timestamp] {
    override val schema = Schema.create(Schema.Type.LONG)
    LogicalTypes.timestampMillis().addToSchema(schema)
  }

  implicit object LocalTimeFor extends SchemaFor[LocalTime] {
    override val schema = Schema.create(Schema.Type.INT)
    LogicalTypes.timeMillis().addToSchema(schema)
  }

  implicit object LocalDateFor extends SchemaFor[LocalDate] {
    override val schema = Schema.create(Schema.Type.INT)
    LogicalTypes.date().addToSchema(schema)
  }

  implicit object LocalDateTimeFor extends SchemaFor[LocalDateTime] {
    override val schema = Schema.create(Schema.Type.LONG)
    LogicalTypes.timestampMillis().addToSchema(schema)
  }

  implicit object InstantFor extends SchemaFor[Instant] {
    override val schema = Schema.create(Schema.Type.LONG)
    LogicalTypes.timestampMillis().addToSchema(schema)
  }

  implicit def javaEnumFor[E <: Enum[_]](implicit tag: ClassTag[E]): SchemaFor[E] = new SchemaFor[E] {
    override val schema: Schema = {

      val annos = tag.runtimeClass.getAnnotations.map { a =>
        Anno(a.annotationType.getClass.getName, Nil)
      }

      val extractor = new AnnotationExtractors(annos)

      val name = tag.runtimeClass.getSimpleName
      val namespace = extractor.namespace.getOrElse(tag.runtimeClass.getPackage.getName)
      val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)

      SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
    }
  }

  implicit def scalaEnumToSchema[E <: scala.Enumeration#Value](implicit tag: TypeTag[E]): SchemaFor[E] = new SchemaFor[E] {

    val typeRef = tag.tpe match {
      case t@TypeRef(_, _, _) => t
    }

    val annos = typeRef.pre.typeSymbol.annotations.map { a =>
      val name = a.tree.tpe.typeSymbol.fullName
      val args = a.tree.children.tail.map(_.toString.stripPrefix("\"").stripSuffix("\""))
      com.sksamuel.avro4s.internal.Anno(name, args)
    }
    val extractor = new AnnotationExtractors(annos)

    val classname = typeRef.pre.typeSymbol.asClass.fullName.stripSuffix(".Value")
    val clazz = Class.forName(classname)

    val namespace = extractor.namespace.getOrElse(clazz.getPackage.getName)
    val name = extractor.name.getOrElse(clazz.getSimpleName)

    val enumClass = Class.forName(classname)
    val symbols = enumClass.getMethod("values").invoke(null).asInstanceOf[scala.Enumeration#ValueSet].iterator.toList.map(_.toString)

    override val schema: Schema = SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
  }

  // This SchemaFor is used for sealed traits of objects
  implicit def genTraitObjectEnum[T, C <: Coproduct, L <: HList](implicit ct: ClassTag[T],
                                                                 tag: TypeTag[T],
                                                                 gen: Generic.Aux[T, C],
                                                                 objs: Reify.Aux[C, L],
                                                                 toList: ToList[L, T]): SchemaFor[T] = new SchemaFor[T] {

    val tpe = weakTypeTag[T]
    val annos = tpe.tpe.typeSymbol.annotations.map { a =>
      val name = a.tree.tpe.typeSymbol.fullName
      val args = a.tree.children.tail.map(_.toString.stripPrefix("\"").stripSuffix("\""))
      Anno(name, args)
    }
    val extractor = new AnnotationExtractors(annos)
    val className = ct.runtimeClass.getCanonicalName
    val packageName = ct.runtimeClass.getPackage.getName
    val namespace = extractor.namespace.getOrElse(packageName)
    val name = extractor.name.getOrElse(ct.runtimeClass.getSimpleName)
    val symbols = toList(objs()).map(_.toString)

    override def schema: Schema = SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
  }

}