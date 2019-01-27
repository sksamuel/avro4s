package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import org.apache.avro.LogicalTypes.{TimeMicros, TimeMillis}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, JsonProperties, LogicalTypes, Schema}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{Coproduct, Generic, HList, Lazy}

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.internal.{Definitions, StdNames, SymbolTable}
import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._

/**
  * A [[Decoder]] is used to convert an Avro value, such as a GenericRecord,
  * SpecificRecord, GenericFixed, EnumSymbol, or a basic JVM type, into a
  * target Scala type.
  *
  * For example, a Decoder[String] would convert an input of type Utf8 -
  * which is one of the ways Avro can encode strings - into a plain Java String.
  *
  * Another example, a decoder for Option[String] would handle inputs of null
  * by emitting a None, and a non-null input by wrapping the decoded value
  * in a Some.
  *
  * A final example is converting a GenericData.Array or a Java collection type
  * into a Scala collection type.
  */
trait Decoder[T] extends Serializable {
  self =>

  /**
    * Decodes the given value to an instance of T if possible.
    * Otherwise throw an error.
    *
    * The provided schema is the reader schema.
    */
  def decode(value: Any, schema: Schema): T

  def map[U](fn: T => U): Decoder[U] = new Decoder[U] {
    override def decode(value: Any, schema: Schema): U = fn(self.decode(value, schema))
  }
}

object Decoder extends CoproductDecoders with TupleDecoders {

  def apply[T](implicit decoder: Decoder[T]): Decoder[T] = decoder

  /**
    * Create a decoder that always returns a single value.
    */
  final def const[A](a: A): Decoder[A] = new Decoder[A] {
    override def decode(value: Any, schema: Schema): A = a
  }

  /**
    * Create a decoder from a function.
    */
  final def instance[A](fn: (Any, Schema) => A): Decoder[A] = new Decoder[A] {
    override def decode(value: Any, schema: Schema): A = fn(value, schema)
  }

  implicit object BooleanDecoder extends Decoder[Boolean] {
    override def decode(value: Any, schema: Schema): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object ByteDecoder extends Decoder[Byte] {
    override def decode(value: Any, schema: Schema): Byte = value.asInstanceOf[Int].toByte
  }

  implicit object ShortDecoder extends Decoder[Short] {
    override def decode(value: Any, schema: Schema): Short = value match {
      case b: Byte => b
      case s: Short => s
      case i: Int => i.toShort
    }
  }

  implicit object ByteArrayDecoder extends Decoder[Array[Byte]] {
    // byte arrays can be encoded multiple ways
    override def decode(value: Any, schema: Schema): Array[Byte] = value match {
      case buffer: ByteBuffer => buffer.array
      case array: Array[Byte] => array
      case fixed: GenericData.Fixed => fixed.bytes()
    }
  }

  implicit object ByteBufferDecoder extends Decoder[ByteBuffer] {
    override def decode(value: Any, schema: Schema): ByteBuffer = ByteArrayDecoder.map(ByteBuffer.wrap).decode(value, schema)
  }

  implicit val ByteListDecoder: Decoder[List[Byte]] = ByteArrayDecoder.map(_.toList)
  implicit val ByteVectorDecoder: Decoder[Vector[Byte]] = ByteArrayDecoder.map(_.toVector)
  implicit val ByteSeqDecoder: Decoder[Seq[Byte]] = ByteArrayDecoder.map(_.toSeq)

  implicit object DoubleDecoder extends Decoder[Double] {
    override def decode(value: Any, schema: Schema): Double = value match {
      case d: Double => d
      case d: java.lang.Double => d
    }
  }

  implicit object FloatDecoder extends Decoder[Float] {
    override def decode(value: Any, schema: Schema): Float = value match {
      case f: Float => f
      case f: java.lang.Float => f
    }
  }

  implicit object IntDecoder extends Decoder[Int] {
    override def decode(value: Any, schema: Schema): Int = value match {
      case byte: Byte => byte.toInt
      case short: Short => short.toInt
      case int: Int => int
      case other => sys.error(s"Cannot convert $other to type INT")
    }
  }

  implicit object LongDecoder extends Decoder[Long] {
    override def decode(value: Any, schema: Schema): Long = value match {
      case byte: Byte => byte.toLong
      case short: Short => short.toLong
      case int: Int => int.toLong
      case long: Long => long
      case other => sys.error(s"Cannot convert $other to type LONG")
    }
  }

  implicit object LocalTimeDecoder extends Decoder[LocalTime] {
    // avro4s stores times as either millis since midnight or micros since midnight
    override def decode(value: Any, schema: Schema): LocalTime = {
      schema.getLogicalType match {
        case _: TimeMicros =>
          value match {
            case i: Int => LocalTime.ofNanoOfDay(i.toLong * 1000L)
            case l: Long => LocalTime.ofNanoOfDay(l * 1000L)
          }
        case _: TimeMillis =>
          value match {
            case i: Int => LocalTime.ofNanoOfDay(i.toLong * 1000000L)
            case l: Long => LocalTime.ofNanoOfDay(l * 1000000L)
          }
      }
    }
  }

  implicit val LocalDateTimeDecoder: Decoder[LocalDateTime] = LongDecoder.map(millis => LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC))
  implicit val LocalDateDecoder: Decoder[LocalDate] = LongDecoder.map(LocalDate.ofEpochDay)
  implicit val InstantDecoder: Decoder[Instant] = LongDecoder.map(Instant.ofEpochMilli)
  implicit val DateDecoder: Decoder[Date] = LocalDateDecoder.map(Date.valueOf)
  implicit val TimestampDecoder: Decoder[Timestamp] = LongDecoder.map(new Timestamp(_))

  implicit object StringDecoder extends Decoder[String] {
    override def decode(value: Any, schema: Schema): String =
      value match {
        case u: Utf8 => u.toString
        case s: String => s
        case charseq: CharSequence => charseq.toString
        case bytebuf: ByteBuffer => new String(bytebuf.array)
        case a: Array[Byte] => new String(a)
        case fixed: GenericData.Fixed => new String(fixed.bytes())
        case null => sys.error("Cannot decode <null> as a string")
        case other => sys.error(s"Cannot decode $other of type ${other.getClass} into a string")
      }
  }

  implicit object UUIDDecoder extends Decoder[UUID] {
    override def decode(value: Any, schema: Schema): UUID = {
      value match {
        case s: String => UUID.fromString(s)
        case u: Utf8 => UUID.fromString(u.toString)
        case bytebuf: ByteBuffer => UUID.fromString(new String(bytebuf.array))
        case a: Array[Byte] => UUID.fromString(new String(a))
        case null => sys.error("Cannot decode <null> as a UUID")
        case other => sys.error(s"Cannot decode $other of type ${other.getClass} into a UUID")
      }
    }
  }

  implicit def optionDecoder[T](implicit decoder: Decoder[T]) = new Decoder[Option[T]] {
    override def decode(value: Any, schema: Schema): Option[T] = if (value == null) None else {
      // Options are Union schemas of ["null", other], the decoder may require the other schema
      val nonNullSchema = schema.getTypes.asScala.filter(_.getType != Schema.Type.NULL).toList match {
        case schema :: Nil => schema
        case multipleSchemas => Schema.createUnion(multipleSchemas.asJava)
      }
      Option(decoder.decode(value, nonNullSchema))
    }
  }

  implicit def vectorDecoder[T](implicit decoder: Decoder[T]): Decoder[Vector[T]] = new Decoder[Vector[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Vector[T] = value match {
      case array: Array[_] => array.toVector.map(decoder.decode(_, schema))
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toVector
      case other => sys.error("Unsupported vector " + other)
    }
  }

  implicit def arrayDecoder[T](implicit decoder: Decoder[T],
                               tag: ClassTag[T]): Decoder[Array[T]] = new Decoder[Array[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Array[T] = value match {
      case array: Array[_] => array.map(decoder.decode(_, schema))
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toArray
      case other => sys.error(s"Unsupported array type ${other.getClass} " + other)
    }
  }

  implicit def setDecoder[T](implicit decoder: Decoder[T]): Decoder[Set[T]] = seqDecoder[T](decoder).map(_.toSet)

  implicit def listDecoder[T](implicit decoder: Decoder[T]): Decoder[List[T]] = new Decoder[List[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): List[T] = value match {
      case array: Array[_] => array.toList.map(decoder.decode(_, schema))
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toList
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def seqDecoder[T](implicit decoder: Decoder[T]): Decoder[Seq[T]] = new Decoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Seq[T] = value match {
      case array: Array[_] => array.toSeq.map(decoder.decode(_, schema))
      case list: java.util.Collection[_] => list.asScala.map(decoder.decode(_, schema)).toSeq
      case other => sys.error("Unsupported array " + other)
    }
  }

  implicit def mapDecoder[T](implicit valueDecoder: Decoder[T]): Decoder[Map[String, T]] = new Decoder[Map[String, T]] {

    import scala.collection.JavaConverters._

    override def decode(value: Any, schema: Schema): Map[String, T] = value match {
      case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> valueDecoder.decode(v, schema) }
      case other => sys.error("Unsupported map " + other)
    }
  }

  implicit object BigDecimalDecoder extends Decoder[BigDecimal] {
    private val decimalConversion = new Conversions.DecimalConversion
    override def decode(value: Any, schema: Schema): BigDecimal =
      // TODO if the encoder is allowing encoding to strings should this not also allow String decoding?
      decimalConversion.fromBytes(value.asInstanceOf[ByteBuffer], schema, schema.getLogicalType)
  }

  implicit def eitherDecoder[A: WeakTypeTag : Decoder, B: WeakTypeTag : Decoder]: Decoder[Either[A, B]] = new Decoder[Either[A, B]] {
    private[this] val safeFromA: SafeFrom[A] = makeSafeFrom[A]
    private[this] val safeFromB: SafeFrom[B] = makeSafeFrom[B]

    override def decode(value: Any, schema: Schema): Either[A, B] = {
      safeFromA.safeFrom(value, schema).map(Left[A, B])
        .orElse(safeFromB.safeFrom(value, schema).map(Right[A, B]))
        .getOrElse {
          val aName = NameResolution(implicitly[WeakTypeTag[A]].tpe)
          val bName = NameResolution(implicitly[WeakTypeTag[B]].tpe)
          sys.error(s"Could not decode $value into Either[$aName, $bName]")
        }
    }
  }

  implicit def javaEnumDecoder[E <: Enum[E]](implicit tag: ClassTag[E]) = new Decoder[E] {
    override def decode(t: Any, schema: Schema): E = {
      Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], t.toString)
    }
  }

  implicit def scalaEnumDecoder[E <: Enumeration#Value](implicit tag: WeakTypeTag[E]) = new Decoder[E] {

    import scala.reflect.NameTransformer._

    val typeRef = tag.tpe match {
      case t@TypeRef(_, _, _) => t
    }

    override def decode(t: Any, schema: Schema): E = {
      val klass = Class.forName(typeRef.pre.typeSymbol.asClass.fullName + "$")
      val enum = klass.getField(MODULE_INSTANCE_NAME).get(null).asInstanceOf[Enumeration]

      enum.withName(t.toString).asInstanceOf[E]
    }
  }

  implicit def genCoproductSingletons[T, C <: Coproduct, L <: HList](implicit gen: Generic.Aux[T, C],
                                                                     objs: Reify.Aux[C, L],
                                                                     toList: ToList[L, T]): Decoder[T] = new Decoder[T] {
    override def decode(value: Any, schema: Schema): T = {
      val name = value.toString
      val variants = toList(objs())
      variants.find(v => {
        getTypeName(v.getClass) == name
      }).getOrElse(sys.error(s"Unknown type $name"))
    }

    private def getTypeName[G](clazz: Class[G]): String = {
      val mirror = runtimeMirror(clazz.getClassLoader)
      val tpe = mirror.classSymbol(clazz).toType
      AvroNameResolver.forClass(tpe).toString
    }

  }

  implicit def applyMacro[T <: Product]: Decoder[T] = macro applyMacroImpl[T]

  def applyMacroImpl[T <: Product : c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[Decoder[T]] = {

    import c.universe
    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe
    val fullName = tpe.typeSymbol.fullName
    val packageName = reflect.packageName(tpe)

    if (!reflect.isCaseClass(tpe)) {
      c.abort(c.enclosingPosition.pos, s"This macro can only encode case classes, not instance of $tpe")
    } else if (reflect.isSealed(tpe)) {
      c.abort(c.prefix.tree.pos, s"$fullName is sealed: Sealed traits/classes should be handled by coproduct generic")
    } else if (packageName.startsWith("scala")) {
      c.abort(c.prefix.tree.pos, s"$fullName is a scala type: Built in types should be handled by explicit typeclasses of SchemaFor and not this macro")
    } else {

      val valueType = reflect.isValueClass(tpe)

      // In Scala, value types are erased at compile time. So in avro4s we assume that value types do not exist
      // in the avro side, neither in the schema, nor in the input values. Therefore, when creating a decoder
      // for a value type, the input will be a value of the underlying type. In other words, given a value type
      // `Foo(s: String) extends AnyVal` - the input will be a String, not a record containing a String
      // as it would be for a non-value type.

      // So the generated decoder for a value type should simply pass through to a generated decoder for
      // the underlying type without worrying about fields etc.
      if (valueType) {

        val valueCstr = tpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten.head
        val backingFieldTpe = valueCstr.typeSignature

        c.Expr[Decoder[T]](
          q"""
          new _root_.com.sksamuel.avro4s.Decoder[$tpe] {
            override def decode(value: Any, schema: _root_.org.apache.avro.Schema): $tpe = {
              val decoded = _root_.com.sksamuel.avro4s.Decoder.decodeT[$backingFieldTpe](value, schema)
              new $tpe(decoded)
            }
          }
          """
        )

      } else {

        // the companion object where the apply construction method is located
        // without this we cannot instantiate the case class
        // we also need this to extract default values
        val companion = tpe.typeSymbol.companion

        if (companion == NoSymbol) {
          val error = s"Cannot find companion object for $fullName; If you have defined a local case class, move the definition to a top level scope."
          Console.err.println(error)
          c.error(c.enclosingPosition.pos, error)
          sys.error(error)
        }

        val decoders = reflect.constructorParameters(tpe).map { case (_, fieldTpe) =>
          if (reflect.isMacroGenerated(fieldTpe)) {
            q"""implicitly[_root_.com.sksamuel.avro4s.Decoder[$fieldTpe]]"""
          } else {
            q"""implicitly[_root_.shapeless.Lazy[_root_.com.sksamuel.avro4s.Decoder[$fieldTpe]]]"""
          }
        }

        val fields = reflect.constructorParameters(tpe).zipWithIndex.map { case ((fieldSym, fieldTpe), index) =>

          // this is the simple name of the field
          val name = fieldSym.name.decodedName.toString

          // the name we use to pull the value out of the record should take into
          // account any @AvroName annotations. If @AvroName is not defined then we
          // use the name of the field in the case class
          val resolvedFieldName = new AnnotationExtractors(reflect.annotations(fieldSym)).name.getOrElse(name)

          val defswithsymbols = universe.asInstanceOf[Definitions with SymbolTable with StdNames]
          val defaultGetterName = defswithsymbols.nme.defaultGetterName(defswithsymbols.nme.CONSTRUCTOR, index + 1)
          val defaultGetterMethod = tpe.companion.member(TermName(defaultGetterName.toString))

          val transient = reflect.isTransientOnField(tpe, fieldSym)

          // if the default is defined, we will use that to populate, otherwise if the field is transient
          // we will populate with none or null, otherwise an error will be raised
          if (defaultGetterMethod.isMethod) {
            if (reflect.isMacroGenerated(fieldTpe)) {
              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefaultNotLazy[$fieldTpe]($resolvedFieldName, record, schema, $companion.$defaultGetterMethod: $fieldTpe, $transient)(decoders($index).asInstanceOf[_root_.com.sksamuel.avro4s.Decoder[$fieldTpe]])"""
            } else {
              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefaultLazy[$fieldTpe]($resolvedFieldName, record, schema, $companion.$defaultGetterMethod: $fieldTpe, $transient)(decoders($index).asInstanceOf[_root_.shapeless.Lazy[_root_.com.sksamuel.avro4s.Decoder[$fieldTpe]]])"""
            }
          } else {
            if (reflect.isMacroGenerated(fieldTpe)) {
              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefaultNotLazy[$fieldTpe]($resolvedFieldName, record, schema, null, $transient)(decoders($index).asInstanceOf[_root_.com.sksamuel.avro4s.Decoder[$fieldTpe]])"""
            } else {
              q"""_root_.com.sksamuel.avro4s.Decoder.decodeFieldOrApplyDefaultLazy[$fieldTpe]($resolvedFieldName, record, schema, null, $transient)(decoders($index).asInstanceOf[_root_.shapeless.Lazy[_root_.com.sksamuel.avro4s.Decoder[$fieldTpe]]])"""
            }
          }
        }

        c.Expr[Decoder[T]](
          q"""
          new _root_.com.sksamuel.avro4s.Decoder[$tpe] {
            private[this] val decoders = Array(..$decoders)

            override def decode(value: Any, schema: _root_.org.apache.avro.Schema): $tpe = {
              val fullName = $fullName
              value match {
                case record: _root_.org.apache.avro.generic.GenericRecord => $companion.apply(..$fields)
                case _ => sys.error("This decoder decodes GenericRecord => " + fullName + " but has been invoked with " + value)
              }
            }
          }
          """
        )
      }
    }
  }

  def decodeFieldOrApplyDefaultNotLazy[T](fieldName: String,
                                       record: GenericRecord,
                                       readerSchema: Schema,
                                       scalaDefault: Any,
                                       transient: Boolean)
                                      (implicit decoder: Decoder[T]): T = {
    decodeFieldOrApplyDefault(fieldName, record, readerSchema, scalaDefault, transient, decoder)
  }

  def decodeFieldOrApplyDefaultLazy[T](fieldName: String,
                                          record: GenericRecord,
                                          readerSchema: Schema,
                                          scalaDefault: Any,
                                          transient: Boolean)
                                         (implicit decoder: Lazy[Decoder[T]]): T = {
    decodeFieldOrApplyDefault(fieldName, record, readerSchema, scalaDefault, transient, decoder.value)
  }

  /**
    * For a field in the target type (the case class we are marshalling to), we must
    * try to pull a value from the Avro GenericRecord. After the value has been retrieved,
    * it needs to be decoded into the appropriate Scala type.
    *
    * If the writer schema does not have an entry for the field then we can consider schema
    * evolution using the following rules in the given order.
    *
    * 1. If the reader schema contains a default for this field, we will use that default.
    * 2. If the parameter is defined with a scala default method then we will use that default value.
    * 3. If the field is marked as @transient
    *
    * If none of these rules can be satisfied then an exception will be thrown.
    */
  //noinspection TypeCheckCanBeMatch
  def decodeFieldOrApplyDefault[T](fieldName: String,
                                   record: GenericRecord,
                                   readerSchema: Schema,
                                   scalaDefault: Any,
                                   transient: Boolean,
                                   decoder: Decoder[T]): T = {

    implicit class RichSchema(s: Schema) {
      def hasField(fieldName: String): Boolean = s.getField(fieldName) != null
      def hasDefault(fieldName: String): Boolean = {
        val field = s.getField(fieldName)
        field != null && field.defaultVal() != null
      }
    }

    record.getSchema.getField(fieldName) match {
      case field: Schema.Field =>
        val value = record.get(fieldName)
        decoder.decode(value, field.schema)
      case null if readerSchema.hasDefault(fieldName) =>
        val default = readerSchema.getField(fieldName).defaultVal() match {
          case JsonProperties.NULL_VALUE => null
          case other => other
        }
        decoder.decode(default, readerSchema.getField(fieldName).schema)
      case null if scalaDefault != null => scalaDefault.asInstanceOf[T]
      case null if transient => None.asInstanceOf[T]
      case _ => sys.error(s"Record $record does not have a value for $fieldName, no default was defined, and the field is not transient")
    }
  }

  def decodeT[T](value: Any, schema: Schema)(implicit decoder: Decoder[T]): T = decoder.decode(value, schema)
}
