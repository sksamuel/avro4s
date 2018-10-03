package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.{Conversions, LogicalTypes, Schema}
import shapeless.ops.coproduct.Reify
import shapeless.ops.hlist.ToList
import shapeless.{Coproduct, Generic, HList}

import scala.language.experimental.macros
import scala.math.BigDecimal.RoundingMode
import scala.reflect.ClassTag

/**
  * An [[Encoder]] encodes a Scala value of type T into a compatible
  * Avro value based on the given schema.
  *
  * For example, given a string value, and a Schema.Type.STRING schema
  * then the string would be encoded as an instance of Utf8, whereas
  * the same string and a Schema.Type.FIXED would be encoded as an
  *
  * For example, an Encoder[String] would encode a string as an instance
  * of Utf8 if the schema was Schema.Type.STRING, whereas for a
  * Schema.Type.FIXED schema, it wuld return a GenericData.Fixed
  */
trait Encoder[T] extends Serializable {
  self =>

  def encode(t: T, schema: Schema): AnyRef

  def comap[S](fn: S => T): Encoder[S] = new Encoder[S] {
    override def encode(value: S, schema: Schema): AnyRef = self.encode(fn(value), schema)
  }
}

case class Exported[A](instance: A) extends AnyVal

object Encoder extends LowPriorityEncoders {

  def apply[T](implicit encoder: Encoder[T]): Encoder[T] = encoder

  implicit def genCoproductSingletons[T, C <: Coproduct, L <: HList](implicit ct: ClassTag[T], gen: Generic.Aux[T, C],
                                                                     objs: Reify.Aux[C, L], toList: ToList[L, T]): Encoder[T] = new Encoder[T] {

    import scala.collection.JavaConverters._
    import scala.reflect.runtime.universe._

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

    override def encode(value: T, schema: Schema): EnumSymbol = new EnumSymbol(schema, value.toString)
  }

  implicit object StringEncoder extends Encoder[String] {
    override def encode(value: String, schema: Schema): AnyRef = {
      schema.getType match {
        case Schema.Type.FIXED => new GenericData.Fixed(schema, value.getBytes)
        case Schema.Type.BYTES => ByteBuffer.wrap(value.getBytes)
        case Schema.Type.STRING => value
        case _ => sys.error(s"Unable to encode a String for schema $schema")
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

  implicit val UUIDEncoder = StringEncoder.comap[UUID](_.toString)
  implicit val LocalTimeEncoder = IntEncoder.comap[LocalTime](lt => lt.toSecondOfDay * 1000 + lt.getNano / 1000)
  implicit val LocalDateEncoder = IntEncoder.comap[LocalDate](_.toEpochDay.toInt)
  implicit val InstantEncoder = LongEncoder.comap[Instant](_.toEpochMilli)
  implicit val LocalDateTimeEncoder = InstantEncoder.comap[LocalDateTime](_.toInstant(ZoneOffset.UTC))
  implicit val TimestampEncoder = InstantEncoder.comap[Timestamp](_.toInstant)
  implicit val DateEncoder = LocalDateEncoder.comap[Date](_.toLocalDate)

  implicit def mapEncoder[V](implicit encoder: Encoder[V]): Encoder[Map[String, V]] = new Encoder[Map[String, V]] {

    import scala.collection.JavaConverters._

    override def encode(map: Map[String, V], schema: Schema): java.util.Map[String, AnyRef] = {
      require(schema != null)
      map.mapValues(encoder.encode(_, schema.getValueType)).asJava
    }
  }

  implicit def listEncoder[T](implicit encoder: Encoder[T]): Encoder[List[T]] = new Encoder[List[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: List[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
      ts.map(encoder.encode(_, arraySchema.getElementType)).asJava
    }
  }

  implicit def setEncoder[T](implicit encoder: Encoder[T]): Encoder[Set[T]] = new Encoder[Set[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Set[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
      ts.map(encoder.encode(_, arraySchema.getElementType)).toList.asJava
    }
  }

  implicit def vectorEncoder[T](implicit encoder: Encoder[T]): Encoder[Vector[T]] = new Encoder[Vector[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Vector[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
      ts.map(encoder.encode(_, arraySchema.getElementType)).asJava
    }
  }

  implicit def seqEncoder[T](implicit encoder: Encoder[T]): Encoder[Seq[T]] = new Encoder[Seq[T]] {

    import scala.collection.JavaConverters._

    override def encode(ts: Seq[T], schema: Schema): java.util.List[AnyRef] = {
      require(schema != null)
      val arraySchema = extractSchemaFromPossibleUnion(schema, Schema.Type.ARRAY)
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

  implicit val ByteListEncoder = ByteArrayEncoder.comap[List[Byte]](_.toArray[Byte])
  implicit val ByteSeqEncoder = ByteArrayEncoder.comap[Seq[Byte]](_.toArray[Byte])
  implicit val ByteVectorEncoder = ByteArrayEncoder.comap[Vector[Byte]](_.toArray[Byte])

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
      // if the option is none we just return null, otherwise we encode the value
      // by finding the non null schema
      val nonNullSchema = schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get
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
    val isValueClass = reflect.isValueClass(tpe)

    // we only encode case classes here
    if (!reflect.isCaseClass(tpe)) {
      c.abort(c.enclosingPosition.pos, "This macro only encodes case classes")
    } else if (reflect.isSealed(tpe)) {
      c.abort(c.prefix.tree.pos, s"$fullName is sealed: Sealed traits/classes should be handled by coproduct generic")
    } else {

      // each field needs to be converted into an avro compatible value
      // so scala primitives need to be converted to java boxed values, and
      // annotations and logical types need to be taken into account

      // We get the value for the field from the class by invoking the
      // getter through t.$name, and then pass that value, and the schema for
      // the record to an Encoder[<Type For Field>] which will then "encode"
      // the value in an avro friendly way.

      // Note: If the field is a value class, then this macro will be summoned again
      // and the value type will be the type argument to the macro.
      val fields = reflect.fieldsOf(tpe).zipWithIndex.map { case ((f, fieldTpe), index) =>
        val name = f.name.asInstanceOf[c.TermName]
        q"""_root_.com.sksamuel.avro4s.Encoder.encodeField[$fieldTpe](t.$name, $index, schema, $fullName)"""
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
                _root_.com.sksamuel.avro4s.Encoder.buildRecord(schema, Seq(..$fields), $fullName)
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
    */
  def buildRecord(schema: Schema, values: Seq[AnyRef], containingClassFQN: String): AnyRef = {
    schema.getType match {
      case Schema.Type.UNION =>
        val subschema = extractTraitSubschema(containingClassFQN, schema)
        ImmutableRecord(subschema, values.toVector)
      case Schema.Type.RECORD =>
        ImmutableRecord(schema, values.toVector)
      case _ =>
        sys.error(s"Trying to encode a field from schema $schema which is neither a RECORD nor a UNION")
    }
  }

  /**
    * Encodes a field in a case class by bringing in an implicit encoder for the fields type.
    * The schema passed in here is the schema for the containing type,
    * and the index is the position of the field in the list of fields of the case class.
    *
    * Note: The field may be a member of a subclass of a trait, in which case
    * the schema passed in will be a union. Therefore we must extract the correct
    * subschema from the union. We can do this by using the name of the
    * containing class, and comparing to the record full names in the subschemas.
    *
    */
  def encodeField[T](t: T, index: Int, schema: Schema, containingClassFQN: String)(implicit encoder: Encoder[T]): AnyRef = {
    schema.getType match {
      case Schema.Type.UNION =>
        val subschema = extractTraitSubschema(containingClassFQN, schema)
        val field = subschema.getFields.get(index)
        encoder.encode(t, field.schema)
      case Schema.Type.RECORD =>
        val field = schema.getFields.get(index)
        encoder.encode(t, field.schema)
      // otherwise we are encoding a simple field
      case _ =>
        encoder.encode(t, schema)
    }
  }

  def extractTraitSubschema(implClassName: String, schema: Schema): Schema = {
    import scala.collection.JavaConverters._
    require(schema.getType == Schema.Type.UNION, "Can only extract subschemas from a UNION")
    schema.getTypes.asScala
      .find(_.getFullName == implClassName)
      .getOrElse(sys.error(s"Cannot find subschema for class $implClassName"))
  }

  def extractSchemaFromPossibleUnion(schema: Schema, `type`: Schema.Type): Schema = {
    import scala.collection.JavaConverters._
    schema.getType match {
      case Schema.Type.UNION => schema.getTypes.asScala.find(_.getType == `type`).getOrElse(sys.error(s"Could not find a schema of type ${`type`} in union $schema"))
      case Schema.Type.ARRAY => schema
      case _ => sys.error(s"Require ARRAY schema but was $schema")
    }
  }
}
