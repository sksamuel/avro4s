package com.sksamuel.avro4s.internal

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util
import java.util.{Collections, UUID}

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
trait SchemaFor[T] {
  def schema: Schema
}

trait LowPrioritySchemaFor {

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))
  // so here we define the schema for the base case of the recursion, C :+: CNil
  implicit def coproductBaseSchema[S](implicit basefor: SchemaFor[S]): SchemaFor[S :+: CNil] = new SchemaFor[S :+: CNil] {
    override def schema: Schema = Schema.createUnion(Collections.singletonList(basefor.schema))
  }

  // And here we continue the recursion up.
  implicit def coproductSchema[S, T <: Coproduct](implicit basefor: SchemaFor[S], coproductFor: SchemaFor[T]): SchemaFor[S :+: T] = new SchemaFor[S :+: T] {
    // union schemas can't contain other union schemas as a direct
    // child, so whenever we create a union, we need to check if our
    // children are unions and flatten
    override def schema: Schema = Schema.createUnion(util.Arrays.asList(basefor.schema, coproductFor.schema))
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

  implicit def apply[T]: SchemaFor[T] = macro applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: whitebox.Context): c.Expr[SchemaFor[T]] = {

    import c.universe
    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeOf[T]

    // we can only encode concrete classes at the top level
    require(tpe.typeSymbol.isClass, tpe + " is not a class but is " + tpe.typeSymbol.fullName)

    val valueType = reflect.isValueClass(tpe)

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
        q"""{ _root_.com.sksamuel.avro4s.internal.SchemaFor.schemaField[$fieldTpe]($fieldName, Seq(..$annos), $moduleSym.$defaultGetterMethod) }"""
      } else {
        q"""{ _root_.com.sksamuel.avro4s.internal.SchemaFor.schemaField[$fieldTpe]($fieldName, Seq(..$annos), null) }"""
      }
    }

    // qualified name of the class we are building
    // todo encode generics:: + genericNameSuffix(underlyingType)
    //val className = tpe.typeSymbol.fullName.toString

    // eg, "Foo" for x.y.Foo
    val simpleName = tpe.typeSymbol.name.decodedName.toString

    // we iterate up the owner tree until we find an Object or Package
    val packageName = Stream.iterate(tpe.typeSymbol.owner)(_.owner)
      .dropWhile(x => !x.isPackage && !x.isModuleClass)
      .head
      .fullName

    // these are annotations on the class itself
    val annos = reflect.annotationsqq(tpe.typeSymbol)

    c.Expr[SchemaFor[T]](
      q"""
        new _root_.com.sksamuel.avro4s.internal.SchemaFor[$tpe] {
          private val _schema = _root_.com.sksamuel.avro4s.internal.SchemaFor.buildSchema($simpleName, $packageName, Seq(..$fields), Seq(..$annos), $valueType)
          override def schema: _root_.org.apache.avro.Schema = _schema
        }
      """)
  }

  /**
    * Builds a [[StructField]] with the data type provided by an implicit instance of [[SchemaFor]].
    * There must be a provider in scope for any type we want to support in avro4s.
    */
  def schemaField[B](name: String, annos: Seq[Anno], default: Any)(implicit schemaFor: SchemaFor[B]): Schema.Field = {
    //  println(s"default for $name = $default")
    new Schema.Field(name, schemaFor.schema, null, null: AnyRef)
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

    if (valueType) {
      // we can only have a single field in a value type
      val field = fields.head
      // if we have a value type AND fixed is non empty, then we return a schema of type fixed
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

  implicit object ByteFor extends SchemaFor[Byte] {
    override val schema: Schema = SchemaBuilder.builder().intType()
  }

  implicit object StringFor extends SchemaFor[String] {
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

  implicit object ShortFor extends SchemaFor[Short] {
    override val schema: Schema = SchemaBuilder.builder().intType()
  }

  implicit object UUIDFor extends SchemaFor[UUID] {
    override val schema: Schema = SchemaBuilder.builder().bytesType()
  }

  implicit object ByteArrayFor extends SchemaFor[Array[Byte]] {
    override val schema: Schema = SchemaBuilder.builder().bytesType()
  }

  implicit object ByteSeqFor extends SchemaFor[Seq[Byte]] {
    override val schema: Schema = SchemaBuilder.builder().bytesType()
  }

  implicit def bigDecimalFor(implicit sp: ScalePrecisionRoundingMode = ScalePrecisionRoundingMode.default): SchemaFor[BigDecimal] = new SchemaFor[BigDecimal] {
    override def schema: Schema = SchemaBuilder.builder().stringType()
  }

  implicit def EitherFor[A, B](implicit leftFor: SchemaFor[A], rightFor: SchemaFor[B]): SchemaFor[Either[A, B]] = {
    new SchemaFor[Either[A, B]] {
      override val schema: Schema = Schema.createUnion(leftFor.schema, rightFor.schema)
    }
  }

  implicit def OptionFor[T](implicit elementType: SchemaFor[T]): SchemaFor[Option[T]] = {
    new SchemaFor[Option[T]] {
      override val schema: Schema = Schema.createUnion(SchemaBuilder.builder().nullType(), elementType.schema)
    }
  }

  implicit def ArrayFor[S](implicit elementType: SchemaFor[S]): SchemaFor[Array[S]] = {
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
      SchemaBuilder.record("Tuple2").namespace("scala").doc(null)
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