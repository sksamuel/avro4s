package com.sksamuel.avro4s

import java.io.{File, InputStream}
import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import shapeless._
import shapeless.labelled._
import shapeless.ops.record._

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait ToSchema[-T] {
  def apply(): Schema
}

object ToSchema {

  implicit object BigDecimalToSchema extends ToSchema[BigDecimal] {
    override def apply(): Schema = {
      val schema = Schema.create(Schema.Type.BYTES)
      schema.addProp("logicalType", "decimal")
      schema.addProp("scale", "2")
      schema.addProp("precision", "8")
      schema
    }
  }

  implicit object BooleanToSchema extends ToSchema[Boolean] {
    override def apply(): Schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit object ByteArrayToSchema extends ToSchema[Array[Byte]] {
    override def apply(): Schema = Schema.create(Schema.Type.BYTES)
  }

  implicit object DoubleToSchema extends ToSchema[Double] {
    override def apply(): Schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit object FloatToSchema extends ToSchema[Float] {
    override def apply(): Schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit object IntToSchema extends ToSchema[Int] {
    override def apply(): Schema = Schema.create(Schema.Type.INT)
  }

  implicit object LongToSchema extends ToSchema[Long] {
    override def apply(): Schema = Schema.create(Schema.Type.LONG)
  }

  implicit object StringToSchema extends ToSchema[String] {
    override def apply(): Schema = Schema.create(Schema.Type.STRING)
  }

  implicit def OptionToSchema[T](implicit scheme: ToSchema[T]): ToSchema[Option[T]] = new ToSchema[Option[T]] {
    override def apply(): Schema = Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), scheme()))
  }

  implicit def MapToSchema[T](implicit tschema: ToSchema[T]): ToSchema[Map[String, T]] = new ToSchema[Map[String, T]] {
    override def apply(): Schema = Schema.createMap(tschema())
  }

  implicit def SetToSchema[T](implicit tschema: ToSchema[T]): ToSchema[Set[T]] = new ToSchema[Set[T]] {
    override def apply(): Schema = Schema.createArray(tschema())
  }

  implicit def EnumToSchema[E <: Enum[E]](implicit tag: ClassTag[E]): ToSchema[Enum[E]] = new ToSchema[Enum[E]] {
    override def apply(): Schema = {
      import scala.collection.JavaConverters._
      val values = tag.runtimeClass.getEnumConstants.map(_.toString)
      Schema.createEnum(tag.runtimeClass.getSimpleName, null, tag.runtimeClass.getPackage.getName, values.toList.asJava)
    }
  }

  implicit def ArrayToSchema[T](implicit tschema: ToSchema[T]): ToSchema[Array[T]] = new ToSchema[Array[T]] {
    override def apply(): Schema = Schema.createArray(tschema())
  }

  implicit def SeqToSchema[T](implicit tschema: ToSchema[T]): ToSchema[Seq[T]] = new ToSchema[Seq[T]] {
    override def apply(): Schema = Schema.createArray(tschema())
  }

  implicit def EitherToSchema[A: ToSchema, B: ToSchema]: ToSchema[Either[A, B]] = new ToSchema[Either[A, B]] {
    override def apply(): Schema = {
      val t = implicitly[ToSchema[A]].apply()
      val u = implicitly[ToSchema[B]].apply()
      Schema.createUnion(util.Arrays.asList(t, u))
    }
  }

  implicit def GenericToSchema[T](implicit tschema: AvroSchema[T]) = new ToSchema[T] {
    override def apply(): Schema = tschema()
  }
}

trait AvroSchemaFields[L <: HList] extends DepFn2[Seq[FieldAnnotation], Seq[FieldAnnotation]] with Serializable {
  type Out = List[Schema.Field]
}

object AvroSchemaFields {

  implicit object HNilFields extends AvroSchemaFields[HNil] {
    override def apply(a: Seq[FieldAnnotation], b: Seq[FieldAnnotation]): List[Field] = List.empty
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       toschema: Lazy[ToSchema[V]],
                                                       remaining: AvroSchemaFields[T]): AvroSchemaFields[FieldType[K, V] :: T] = {

    new AvroSchemaFields[FieldType[K, V] :: T] {
      def apply(docs: Seq[FieldAnnotation], props: Seq[FieldAnnotation]): List[Schema.Field] = {
        val name = key.value.name
        val doc = docs.find(_.field == name).flatMap(_.values.headOption).orNull
        val field = new Schema.Field(name, toschema.value(), doc, null)
        field +: remaining(docs, props)
      }
    }
  }
}

trait AvroSchema[T] {
  def apply(): Schema
}

case class FieldAnnotation(field: String, values: Seq[String])

object AvroSchema {

  import scala.reflect.ClassTag
  import scala.reflect.runtime.universe.{Type, WeakTypeTag, typeOf}

  implicit def GenericAvroSchema[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                                   keys: Keys[Repr],
                                                   schemaFields: AvroSchemaFields[Repr],
                                                   typeTag: WeakTypeTag[T],
                                                   annotations: Annotations[AvroDoc, T],
                                                   tag: ClassTag[T]): AvroSchema[T] = new AvroSchema[T] {

    def annotations(tpe: Type): Seq[FieldAnnotation] = {
      typeTag.tpe.members.filter(_.isConstructor).head.asMethod.paramLists.flatten.map { sym =>
        FieldAnnotation(
          sym.name.decodedName.toString,
          sym.annotations.collect {
            case a if a.tree.tpe <:< tpe => a.tree.children.tail.head.toString.drop(1).dropRight(1)
          }
        )
      }
    }

    val docs = annotations(typeOf[AvroDoc])
    val props = annotations(typeOf[AvroProp])

    import scala.collection.JavaConverters._

    def apply(): Schema = {
      val schema = org.apache.avro.Schema.createRecord(
        typeTag.tpe.typeSymbol.name.toString,
        typeTag.tpe.typeSymbol.annotations.collectFirst {
          case a if a.tree.tpe.<:<(typeOf[AvroDoc]) => a.tree.children.tail.head.toString.drop(1).dropRight(1)
        }.orNull,
        tag.runtimeClass.getPackage.getName,
        false
      )
      schema.setFields(schemaFields(docs, props).asJava)
      schema
    }
  }

  def apply[T](implicit builder: Lazy[AvroSchema[T]]): Schema = builder.value.apply()

  def apply(str: String): Schema = new Schema.Parser().parse(str)
  def apply(is: InputStream): Schema = new Schema.Parser().parse(is)
  def apply(file: File): Schema = new Schema.Parser().parse(file)
}