package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema
import shapeless.labelled._
import shapeless._

trait ToSchema[T] {
  def schema: Option[Schema]
}

object ToSchema {

  implicit object BooleanToSchema extends ToSchema[Boolean] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.BOOLEAN))
  }

  implicit object ByteArrayToSchema extends ToSchema[Array[Byte]] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.BYTES))
  }

  implicit object DoubleToSchema extends ToSchema[Double] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.DOUBLE))
  }

  implicit object FloatToSchema extends ToSchema[Float] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.FLOAT))
  }

  implicit object IntToSchema extends ToSchema[Int] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.INT))
  }

  implicit object LongToSchema extends ToSchema[Long] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.LONG))
  }

  implicit object StringToSchema extends ToSchema[String] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.STRING))
  }

  implicit def OptionToSchema[T](implicit scheme: ToSchema[T]): ToSchema[Option[T]] = new ToSchema[Option[T]] {
    override def schema: Option[Schema] = {
      scheme.schema.map { schema => Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), schema)) }
    }
  }

  implicit def MapToSchema[T](implicit scheme: ToSchema[T]): ToSchema[Map[String, T]] = new ToSchema[Map[String, T]] {
    override def schema: Option[Schema] = scheme.schema.map { schema => Schema.createMap(schema) }
  }

  implicit def ListToSchema[T](implicit scheme: ToSchema[T]): ToSchema[List[T]] = new ToSchema[List[T]] {
    override def schema: Option[Schema] = scheme.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def ArrayToSchema[T](implicit scheme: ToSchema[T]): ToSchema[Array[T]] = new ToSchema[Array[T]] {
    override def schema: Option[Schema] = scheme.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def SeqToSchema[T](implicit scheme: ToSchema[T]): ToSchema[Seq[T]] = new ToSchema[Seq[T]] {
    override def schema: Option[Schema] = scheme.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def EitherToSchema[A: ToSchema, B: ToSchema]: ToSchema[Either[A, B]] = new ToSchema[Either[A, B]] {
    override def schema: Option[Schema] = {
      for (t <- implicitly[ToSchema[A]].schema;
           u <- implicitly[ToSchema[B]].schema) yield {
        Schema.createUnion(util.Arrays.asList(t, u))
      }
    }
  }

  implicit def GenericToSchema[T](implicit scheme: AvroSchema2[T]) = new ToSchema[T] {
    override def schema: Option[Schema] = Some(scheme.apply)
  }
}

trait AvroSchemaFields[L <: HList] extends DepFn0 with Serializable {
  type Out = List[Schema.Field]
}

object AvroSchemaFields {

  implicit object HNilFields extends AvroSchemaFields[HNil] {
    def apply() = List.empty
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       builder: Lazy[ToSchema[V]],
                                                       remaining: AvroSchemaFields[T]): AvroSchemaFields[FieldType[K, V] :: T] = {
    new AvroSchemaFields[FieldType[K, V] :: T] {
      def apply: List[Schema.Field] = {
        val fieldFn: (Schema => Schema.Field) = schema => new Schema.Field(key.value.name, schema, null, null)
        builder.value.schema.map(fieldFn).toList ++ remaining()
      }
    }
  }
}

trait AvroSchema2[T] {
  def apply(): Schema
}

object AvroSchema2 {

  import scala.reflect.ClassTag
  import scala.reflect.runtime.universe.typeOf
  import scala.reflect.runtime.universe.WeakTypeTag

  implicit def schemaBuilder[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                               schemaFields: AvroSchemaFields[Repr],
                                               typeTag: WeakTypeTag[T],
                                               tag: ClassTag[T]): AvroSchema2[T] = new AvroSchema2[T] {

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
      schema.setFields(schemaFields().asJava)
      schema
    }
  }

  def apply[T](implicit builder: Lazy[AvroSchema2[T]]): Schema = builder.value.apply()
}