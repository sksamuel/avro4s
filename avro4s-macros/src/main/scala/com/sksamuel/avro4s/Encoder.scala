package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, LogicalTypes, Schema}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{Coproduct, Generic, HList, Lazy}

import scala.language.experimental.macros
import scala.math.BigDecimal.RoundingMode
import scala.reflect.ClassTag

/**
  * An [[Encoder]] encodes a Scala value of type T into a compatible
  * Avro value based on the given schema.
  *
  * For example, given a string, and a schema of type Schema.Type.STRING
  * then the string would be encoded as an instance of Utf8, whereas
  * the same string and a Schema.Type.FIXED would be encoded as an
  * instance of GenericData.Fixed.
  *
  * Another example is given a Scala enumeration value, and a schema of
  * type Schema.Type.ENUM, the value would be encoded as an instance
  * of GenericData.EnumSymbol.
  */
trait Encoder[T] extends Serializable {
  self =>

  def encode(t: T, schema: Schema): AnyRef

  def comap[S](fn: S => T): Encoder[S] = new Encoder[S] {
    override def encode(value: S, schema: Schema): AnyRef = self.encode(fn(value), schema)
  }
}

case class Exported[A](instance: A) extends AnyVal

object Encoder extends CoproductEncoders with TupleEncoders {

  def apply[T](implicit encoder: Encoder[T]): Encoder[T] = encoder

  implicit def genCoproductSingletons[T, C <: Coproduct, L <: HList](implicit ct: ClassTag[T], gen: Generic.Aux[T, C],
                                                                     objs: Reify.Aux[C, L], toList: ToList[L, T]): Encoder[T] = new Encoder[T] {

    import scala.collection.JavaConverters._
    import scala.reflect.runtime.universe._

    protected val schema: Schema = {
      val tpe = weakTypeTag[T]
      val nr = NameResolution(tpe.tpe)
      val symbols = toList(objs()).map(v => NameResolution(v.getClass).name).asJava
      Schema.createEnum(nr.name, null, nr.namespace, symbols)
    }

    override def encode(value: T, schema: Schema): EnumSymbol = new EnumSymbol(
      schema,
      NameResolution(value.getClass).name
    )
  }

  implicit object StringEncoder extends Encoder[String] {
    override def encode(value: String, schema: Schema): AnyRef = {
      schema.getType match {
        case Schema.Type.FIXED => new GenericData.Fixed(schema, value.getBytes)
        case Schema.Type.BYTES => ByteBuffer.wrap(value.getBytes)
        case _ => new Utf8(value)
      }
    }
  }

  implicit object BooleanEncoder extends Encoder[Boolean] {
    override def encode(t: Boolean, schema: Schema): java.lang.Boolean = java.lang.Boolean.valueOf(t)
  }

  implicit object IntEncoder extends Encoder[Int] {
    override def encode(t: Int, schema: Schema): java.lang.Integer = java.lang.Integer.valueOf(t)
  }

  implicit object LongEncoder extends Encoder[Long] {
    override def encode(t: Long, schema: Schema): java.lang.Long = java.lang.Long.valueOf(t)
  }

  implicit object FloatEncoder extends Encoder[Float] {
    override def encode(t: Float, schema: Schema): java.lang.Float = java.lang.Float.valueOf(t)
  }

  implicit object DoubleEncoder extends Encoder[Double] {
    override def encode(t: Double, schema: Schema): java.lang.Double = java.lang.Double.valueOf(t)
  }

  implicit object ShortEncoder extends Encoder[Short] {
    override def encode(t: Short, schema: Schema): java.lang.Short = java.lang.Short.valueOf(t)
  }

  implicit object ByteEncoder extends Encoder[Byte] {
    override def encode(t: Byte, schema: Schema): java.lang.Byte = java.lang.Byte.valueOf(t)
  }

  implicit object NoneEncoder extends Encoder[None.type] {
    override def encode(t: None.type, schema: Schema) = null
  }

  implicit val UUIDEncoder: Encoder[UUID] = StringEncoder.comap[UUID](_.toString)
  implicit val LocalTimeEncoder: Encoder[LocalTime] = IntEncoder.comap[LocalTime](lt => lt.toSecondOfDay * 1000 + lt.getNano / 1000)
  implicit val LocalDateEncoder: Encoder[LocalDate] = IntEncoder.comap[LocalDate](_.toEpochDay.toInt)
  implicit val InstantEncoder: Encoder[Instant] = LongEncoder.comap[Instant](_.toEpochMilli)
  implicit val LocalDateTimeEncoder: Encoder[LocalDateTime] = InstantEncoder.comap[LocalDateTime](_.toInstant(ZoneOffset.UTC))
  implicit val TimestampEncoder: Encoder[Timestamp] = InstantEncoder.comap[Timestamp](_.toInstant)
  implicit val DateEncoder: Encoder[Date] = LocalDateEncoder.comap[Date](_.toLocalDate)

  implicit def mapEncoder[V](implicit encoder: Encoder[V]): Encoder[Map[String, V]] = new Encoder[Map[String, V]] {

    import scala.collection.JavaConverters._

    override def encode(map: Map[String, V], schema: Schema): java.util.Map[String, AnyRef] = {
      require(schema != null)
      map.map { case (k, v) =>
        (k, encoder.encode(v, schema.getValueType))
      }.asJava
    }
  }

  implicit def listEncoder[T](implicit encoder: Encoder[T]): Encoder[List[T]] = new Encoder[List[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: List[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = SchemaHelper.extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
      ts.map(encoder.encode(_, arraySchema.getElementType)).asJava
    }
  }

  implicit def setEncoder[T](implicit encoder: Encoder[T]): Encoder[Set[T]] = new Encoder[Set[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Set[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = SchemaHelper.extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
      ts.map(encoder.encode(_, arraySchema.getElementType)).toList.asJava
    }
  }

  implicit def vectorEncoder[T](implicit encoder: Encoder[T]): Encoder[Vector[T]] = new Encoder[Vector[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Vector[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = SchemaHelper.extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
      ts.map(encoder.encode(_, arraySchema.getElementType)).asJava
    }
  }

  implicit def seqEncoder[T](implicit encoder: Encoder[T]): Encoder[Seq[T]] = new Encoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Seq[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = SchemaHelper.extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
      ts.map(encoder.encode(_, arraySchema.getElementType)).asJava
    }
  }

  implicit object ByteArrayEncoder extends Encoder[Array[Byte]] {
    override def encode(t: Array[Byte], schema: Schema): AnyRef = {
      schema.getType match {
        case Schema.Type.FIXED => new GenericData.Fixed(schema, t)
        case Schema.Type.BYTES => ByteBuffer.wrap(t)
        case _ => sys.error(s"Unable to encode $t for schema $schema")
      }
    }
  }

  implicit val ByteListEncoder: Encoder[List[Byte]] = ByteArrayEncoder.comap(_.toArray[Byte])
  implicit val ByteSeqEncoder: Encoder[Seq[Byte]] = ByteArrayEncoder.comap(_.toArray[Byte])
  implicit val ByteVectorEncoder: Encoder[Vector[Byte]] = ByteArrayEncoder.comap(_.toArray[Byte])

  implicit object ByteBufferEncoder extends Encoder[ByteBuffer] {
    override def encode(t: ByteBuffer, schema: Schema): ByteBuffer = t
  }

  implicit def arrayEncoder[T](implicit encoder: Encoder[T]): Encoder[Array[T]] = new Encoder[Array[T]] {

    import scala.collection.JavaConverters._

    // if our schema is BYTES then we assume the incoming array is a byte array and serialize appropriately
    override def encode(ts: Array[T], schema: Schema): AnyRef = schema.getType match {
      case Schema.Type.BYTES => ByteBuffer.wrap(ts.asInstanceOf[Array[Byte]])
      case _ => ts.map(encoder.encode(_, schema.getElementType)).toList.asJava
    }
  }

  implicit def optionEncoder[T](implicit encoder: Encoder[T]): Encoder[Option[T]] = new Encoder[Option[T]] {

    import scala.collection.JavaConverters._

    override def encode(t: Option[T], schema: Schema): AnyRef = {
      val nonNullSchema = schema.getTypes.size match {
        // if the option is none we just return null, otherwise we encode the value
        // by finding the non null schema
        case 2 => schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get
        case _ =>
          val remainingSchemas = schema.getTypes.asScala.filter(_.getType != Schema.Type.NULL)
          Schema.createUnion(remainingSchemas.toList.asJava)
      }
      t.map(encoder.encode(_, nonNullSchema)).orNull
    }
  }

  implicit def eitherEncoder[T, U](implicit leftEncoder: Encoder[T], rightEncoder: Encoder[U]): Encoder[Either[T, U]] = new Encoder[Either[T, U]] {
    override def encode(t: Either[T, U], schema: Schema): AnyRef = t match {
      case Left(left) => leftEncoder.encode(left, schema.getTypes.get(0))
      case Right(right) => rightEncoder.encode(right, schema.getTypes.get(1))
    }
  }

  implicit object BigDecimalEncoder extends Encoder[BigDecimal] {

    override def encode(t: BigDecimal, schema: Schema): AnyRef = {

      // we support encoding big decimals in three ways - fixed, bytes or as a String
      schema.getType match {
        case Schema.Type.STRING => StringEncoder.encode(t.toString, schema)
        case Schema.Type.BYTES => ByteBufferEncoder.comap[BigDecimal] { _ =>

          val decimal = schema.getLogicalType.asInstanceOf[Decimal]
          require(decimal != null)

          val decimalConversion = new Conversions.DecimalConversion
          val decimalType = LogicalTypes.decimal(decimal.getPrecision, decimal.getScale)

          val scaledValue = t.setScale(decimal.getScale, RoundingMode.HALF_UP)
          decimalConversion.toBytes(scaledValue.bigDecimal, null, decimalType)

        }.encode(t, schema)
        case Schema.Type.FIXED => sys.error("Unsupported. PR Please!")
        case _ => sys.error(s"Cannot serialize BigDecimal as ${schema.getType}")
      }
    }
  }

  implicit def javaEnumEncoder[E <: Enum[_]]: Encoder[E] = new Encoder[E] {
    override def encode(t: E, schema: Schema): EnumSymbol = new EnumSymbol(schema, t.name)
  }

  implicit def scalaEnumEncoder[E <: Enumeration#Value]: Encoder[E] = new Encoder[E] {
    override def encode(t: E, schema: Schema): EnumSymbol = new EnumSymbol(schema, t.toString)
  }

  implicit def applyMacro[T]: Encoder[T] = macro applyMacroImpl[T]

  def applyMacroImpl[T: c.WeakTypeTag](c: scala.reflect.macros.whitebox.Context): c.Expr[Encoder[T]] = {

    import c.universe._

    val reflect = ReflectHelper(c)
    val tpe = weakTypeTag[T].tpe
    val fullName = tpe.typeSymbol.fullName

    if (!reflect.isCaseClass(tpe)) {
      c.abort(c.prefix.tree.pos, s"$fullName is not a case class: This macro is only designed to handle case classes")
    } else if (reflect.isSealed(tpe)) {
      c.abort(c.prefix.tree.pos, s"$fullName is sealed: Sealed traits/classes should be handled by coproduct generic")
    } else if (fullName.startsWith("scala") || fullName.startsWith("java")) {
      c.abort(c.prefix.tree.pos, s"$fullName is a library type: Built in types should be handled by explicit typeclasses of SchemaFor and not this macro")
    } else if (reflect.isScalaEnum(tpe)) {
      c.abort(c.prefix.tree.pos, s"$fullName is a scala enum: Scala enum types should be handled by `scalaEnumSchemaFor`")
    } else {

      val nameResolution = NameResolution(c)(tpe)
      val isValueClass = reflect.isValueClass(tpe)

      // each field needs to be converted into an avro compatible value
      // so scala primitives need to be converted to java boxed values, and
      // annotations and logical types need to be taken into account

      // We get the value for the field from the class by invoking the
      // getter through t.$name, and then pass that value, and the schema for
      // the record to an Encoder[<Type For Field>] which will then "encode"
      // the value in an avro friendly way.

      // Note: If the field is a value class, then this macro will be summoned again
      // and the value type will be the type argument to the macro.
      val fields = reflect
        .constructorParameters(tpe)
        .filterNot { case (fieldSym, _) => reflect.isTransientOnField(tpe, fieldSym) }
        .map { case (fieldSym, fieldTpe) =>
          val termName = fieldSym.asTerm.name

          // we need to check @AvroName annotation on the field, because that will determine the name
          // that we use when looking inside the schema to pull out the field
          val annos = reflect.annotations(fieldSym)

          val isCaseClassOrSealed = reflect.isCaseClass(fieldTpe) || reflect.isSealed(fieldTpe)

          val fieldName = new AnnotationExtractors(annos).name.getOrElse(fieldSym.name.decodedName.toString)
          if (isCaseClassOrSealed) {
            q"""_root_.com.sksamuel.avro4s.Encoder.encodeFieldNotLazy[$fieldTpe](t.$termName, $fieldName, schema, ${nameResolution.fullName})"""
          } else {
            q"""_root_.com.sksamuel.avro4s.Encoder.encodeFieldLazy[$fieldTpe](t.$termName, $fieldName, schema, ${nameResolution.fullName})"""
          }
        }

      // An encoder for a value type just needs to pass through the given value into an encoder
      // for the backing type. At runtime, the value type class won't exist, and the input
      // will be an instance of whatever the backing field of the value class was defined to be.
      if (isValueClass) {
        c.Expr[Encoder[T]](
          q"""
            new _root_.com.sksamuel.avro4s.Encoder[$tpe] {
              override def encode(t: $tpe, schema: org.apache.avro.Schema): AnyRef = Seq(..$fields).head
            }
        """
        )
      } else {
        c.Expr[Encoder[T]](
          q"""
            new _root_.com.sksamuel.avro4s.Encoder[$tpe] {
              override def encode(t: $tpe, schema: org.apache.avro.Schema): AnyRef = {
                _root_.com.sksamuel.avro4s.Encoder.buildRecord(schema, Seq(..$fields), ${nameResolution.fullName})
              }
            }
        """
        )
      }
    }
  }

  /**
    * Takes the encoded values from the fields of a type T and builds
    * an [[ImmutableRecord]] from them, using the given schema.
    *
    * The schema for a record must be of Type Schema.Type.RECORD but
    * the case class may have been a subclass of a trait. In this case
    * the schema will be a union and so we must extract the correct
    * subschema from the union.
    *
    * @param fullName the full name of the record in Avro, taking into
    *                 account Avro modifiers such as @AvroNamespace
    *                 and @AvroErasedName. This name is used for
    *                 extracting the specific subschema from a union schema.
    */
  def buildRecord(schema: Schema, values: Seq[AnyRef], fullName: String): AnyRef = {
    schema.getType match {
      case Schema.Type.UNION =>
        val subschema = SchemaHelper.extractTraitSubschema(fullName, schema)
        ImmutableRecord(subschema, values.toVector)
      case Schema.Type.RECORD =>
        ImmutableRecord(schema, values.toVector)
      case _ =>
        sys.error(s"Trying to encode a field from schema $schema which is neither a RECORD nor a UNION")
    }
  }

  def encodeFieldNotLazy[T](t: T, fieldName: String, schema: Schema, fullName: String)
                           (implicit encoder: Encoder[T]): AnyRef = {
    encodeField(t, fieldName, schema, fullName, encoder)
  }


  def encodeFieldLazy[T](t: T, fieldName: String, schema: Schema, fullName: String)
                        (implicit encoder: Lazy[Encoder[T]]): AnyRef = {
    encodeField(t, fieldName, schema, fullName, encoder.value)
  }

  /**
    * Encodes a field in a case class by bringing in an implicit encoder for the field's type.
    * The schema passed in here is the schema for the container type, and the fieldName
    * is the name of the field in the avro schema.
    *
    * Note: The field may be a member of a subclass of a trait, in which case
    * the schema passed in will be a union. Therefore we must extract the correct
    * subschema from the union. We can do this by using the fullName of the
    * containing class, and comparing to the record full names in the subschemas.
    *
    */
  private def encodeField[T](t: T, fieldName: String, schema: Schema, fullName: String, encoder: Encoder[T]): AnyRef = {
    schema.getType match {
      case Schema.Type.UNION =>
        val subschema = SchemaHelper.extractTraitSubschema(fullName, schema)
        val field = subschema.getField(fieldName)
        encoder.encode(t, field.schema)
      case Schema.Type.RECORD =>
        val field = schema.getField(fieldName)
        encoder.encode(t, field.schema)
      // otherwise we are encoding a simple field
      case _ =>
        encoder.encode(t, schema)
    }
  }
}
