package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema
import shapeless.labelled._
import shapeless._

import scala.reflect.ClassTag


trait FieldWrite[T] {
  def schema: Option[Schema]
}

object FieldWrite {

  implicit object StringFieldWrite extends FieldWrite[String] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.STRING))
  }

  implicit object BooleanFieldWrite extends FieldWrite[Boolean] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.BOOLEAN))
  }

  implicit object IntFieldWrite extends FieldWrite[Int] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.INT))
  }

  implicit object LongFieldWrite extends FieldWrite[Long] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.LONG))
  }

  implicit object DoubleFieldWrite extends FieldWrite[Double] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.DOUBLE))
  }

  implicit object FloatFieldWrite extends FieldWrite[Float] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.FLOAT))
  }

  implicit object HNilFieldWrite extends FieldWrite[HNil] {
    override def schema: Option[Schema] = None
  }

  implicit def RecordFieldWrite[T](implicit builder: SchemaBuilder[T]) = new FieldWrite[T] {
    override def schema: Option[Schema] = Some(builder())
  }

  implicit def OptionFieldWrite[T](implicit write: FieldWrite[T]): FieldWrite[Option[T]] = new FieldWrite[Option[T]] {
    override def schema: Option[Schema] = {
      write.schema.map { schema => Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), schema)) }
    }
  }

  implicit def OptionFieldWriteRecord[T](implicit builder: SchemaBuilder[T]): FieldWrite[Option[T]] = new FieldWrite[Option[T]] {
    override def schema: Option[Schema] = {
      val schema = Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), builder()))
      Some(schema)
    }
  }

  implicit def ListFieldWrite[T](implicit write: FieldWrite[T]): FieldWrite[List[T]] = new FieldWrite[List[T]] {
    override def schema: Option[Schema] = write.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def ArrayFieldWrite[T](implicit write: FieldWrite[T]): FieldWrite[Array[T]] = new FieldWrite[Array[T]] {
    override def schema: Option[Schema] = write.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def SeqFieldWrite[T](implicit write: FieldWrite[T]): FieldWrite[Seq[T]] = new FieldWrite[Seq[T]] {
    override def schema: Option[Schema] = write.schema.map { schema => Schema.createArray(schema) }
  }
}

trait SchemaFields[L <: HList] extends DepFn0 with Serializable {
  type Out = List[Schema.Field]
}

object SchemaFields {

  implicit object HNilFields extends SchemaFields[HNil] {
    def apply() = List.empty
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       write: FieldWrite[V],
                                                       remaining: SchemaFields[T],
                                                       tag: ClassTag[V]): SchemaFields[FieldType[K, V] :: T] = {
    new SchemaFields[FieldType[K, V] :: T] {
      def apply: List[Schema.Field] = {
        val fieldFn: (Schema => Schema.Field) = schema => new Schema.Field(key.value.name, schema, null, null)
        write.schema.map(fieldFn).toList ++ remaining()
      }
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