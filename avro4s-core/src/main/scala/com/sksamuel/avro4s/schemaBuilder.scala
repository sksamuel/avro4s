package com.sksamuel.avro4s

import org.apache.avro.Schema
import shapeless.labelled._
import shapeless._

import scala.reflect.ClassTag

trait SchemaFields[L <: HList] extends DepFn0 with Serializable {
  type Out = List[Schema.Field]
}

object SchemaFields {

  def nested[T](implicit builder: SchemaBuilder[T]): Schema.Field = ???

  def field[K <: Symbol, V](key: Witness.Aux[K], tag: ClassTag[V]): Schema.Field = {
    tag.runtimeClass match {
      case tag if tag == classOf[String] => new Schema.Field(key.value.name, Schema.create(Schema.Type.STRING), null, null)
      case tag if tag == classOf[Boolean] => new Schema.Field(key.value.name, Schema.create(Schema.Type.BOOLEAN), null, null)
      case tag if tag == classOf[Int] => new Schema.Field(key.value.name, Schema.create(Schema.Type.INT), null, null)
      case tag if tag == classOf[Long] => new Schema.Field(key.value.name, Schema.create(Schema.Type.LONG), null, null)
    }
  }

  implicit object HNilFields extends SchemaFields[HNil] {
    def apply() = List.empty
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       remaining: SchemaFields[T],
                                                       tag: ClassTag[V]): SchemaFields[FieldType[K, V] :: T] = {
    new SchemaFields[FieldType[K, V] :: T] {
      def apply() = field(key, tag) :: remaining.apply()
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