package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import shapeless.labelled._
import shapeless._

import scala.reflect.ClassTag


trait FieldWrite[T] {
  def field(name: String): List[Field]
}

object FieldWrite {

  implicit object StringFieldWrite extends FieldWrite[String] {
    override def field(name: String): List[Field] = List(new Schema.Field(name, Schema.create(Schema.Type.STRING), null, null))
  }

  implicit object BooleanFieldWrite extends FieldWrite[Boolean] {
    override def field(name: String): List[Field] = List(new Schema.Field(name, Schema.create(Schema.Type.BOOLEAN), null, null))
  }

  implicit object IntFieldWrite extends FieldWrite[Int] {
    override def field(name: String): List[Field] = List(new Schema.Field(name, Schema.create(Schema.Type.INT), null, null))
  }

  implicit object LongFieldWrite extends FieldWrite[Long] {
    override def field(name: String): List[Field] = List(new Schema.Field(name, Schema.create(Schema.Type.LONG), null, null))
  }

  implicit object DoubleFieldWrite extends FieldWrite[Double] {
    override def field(name: String): List[Field] = List(new Schema.Field(name, Schema.create(Schema.Type.DOUBLE), null, null))
  }

  implicit object FloatFieldWrite extends FieldWrite[Float] {
    override def field(name: String): List[Field] = List(new Schema.Field(name, Schema.create(Schema.Type.FLOAT), null, null))
  }

  implicit object HNilFieldWrite extends FieldWrite[HNil] {
    override def field(name: String): List[Field] = Nil
  }

  implicit def RecordFieldWrite[T](implicit builder: SchemaBuilder[T]) = new FieldWrite[T] {
    override def field(name: String): List[Field] = List(new Schema.Field(name, builder(), null, null))
  }

  implicit def OptionFieldWrite[T](implicit write: FieldWrite[T]): FieldWrite[Option[T]] = new FieldWrite[Option[T]] {
    override def field(name: String): List[Field] = {
      val schema = Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), write.field("dummy").head.schema))
      List(new Schema.Field(name, schema, null, null))
    }
  }

  implicit def OptionFieldWriteRecord[T](implicit builder: SchemaBuilder[T]): FieldWrite[Option[T]] = new FieldWrite[Option[T]] {
    val schema = Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), builder()))
    override def field(name: String): List[Field] = List(new Schema.Field(name, schema, null, null))
  }
}

trait SchemaFields[L <: HList] extends DepFn0 with Serializable {
  type Out = List[Schema.Field]
}

object SchemaFields {

  def nested[T](implicit builder: SchemaBuilder[T]): Schema.Field = ???

  implicit object HNilFields extends SchemaFields[HNil] {
    def apply() = List.empty
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       write: FieldWrite[V],
                                                       remaining: SchemaFields[T],
                                                       tag: ClassTag[V]): SchemaFields[FieldType[K, V] :: T] = {
    new SchemaFields[FieldType[K, V] :: T] {
      def apply() = write.field(key.value.name) ++ remaining.apply()
    }
  }
}

trait SchemaBuilder[T] {
  def apply(): Schema
}

object SchemaBuilder {

  implicit def schemaBuilder[T, Repr <: HList](implicit label: LabelledGeneric.Aux[T, Repr],
                                               schemaFields: SchemaFields[Repr],
                                               tag: ClassTag[T]): SchemaBuilder[T] = new SchemaBuilder[T] {

    import scala.collection.JavaConverters._

    def apply(): Schema = {
      val schema = org.apache.avro.Schema.createRecord(tag.runtimeClass.getSimpleName.takeWhile(_ != '$'), null, tag.runtimeClass.getPackage.getName, false)
      schema.setFields(schemaFields().asJava)
      schema
    }
  }

  def apply[T](implicit builder: SchemaBuilder[T]): Schema = builder.apply()
}