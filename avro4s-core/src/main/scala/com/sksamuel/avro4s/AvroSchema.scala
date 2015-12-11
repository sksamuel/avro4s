package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema
import shapeless.labelled._
import shapeless._

import scala.reflect.ClassTag


trait SchemaBuilder[T] {
  def schema: Option[Schema]
}

object SchemaBuilder {

  implicit object ByteArraySchemaBuilder extends SchemaBuilder[Array[Byte]] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.BYTES))
  }

  implicit object StringSchemaBuilder extends SchemaBuilder[String] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.STRING))
  }

  implicit object BooleanSchemaBuilder extends SchemaBuilder[Boolean] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.BOOLEAN))
  }

  implicit object IntSchemaBuilder extends SchemaBuilder[Int] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.INT))
  }

  implicit object LongSchemaBuilder extends SchemaBuilder[Long] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.LONG))
  }

  implicit object DoubleSchemaBuilder extends SchemaBuilder[Double] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.DOUBLE))
  }

  implicit object FloatSchemaBuilder extends SchemaBuilder[Float] {
    override def schema: Option[Schema] = Some(Schema.create(Schema.Type.FLOAT))
  }

  implicit def OptionFieldWrite[T](implicit builder: SchemaBuilder[T]): SchemaBuilder[Option[T]] = new SchemaBuilder[Option[T]] {
    override def schema: Option[Schema] = {
      builder.schema.map { schema => Schema.createUnion(util.Arrays.asList(Schema.create(Schema.Type.NULL), schema)) }
    }
  }

  implicit def MapFieldWrite[T](implicit builder: SchemaBuilder[T]): SchemaBuilder[Map[String, T]] = new SchemaBuilder[Map[String, T]] {
    override def schema: Option[Schema] = builder.schema.map { schema => Schema.createMap(schema) }
  }

  implicit def ListFieldWrite[T](implicit builder: SchemaBuilder[T]): SchemaBuilder[List[T]] = new SchemaBuilder[List[T]] {
    override def schema: Option[Schema] = builder.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def ArrayFieldWrite[T](implicit builder: SchemaBuilder[T]): SchemaBuilder[Array[T]] = new SchemaBuilder[Array[T]] {
    override def schema: Option[Schema] = builder.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def SeqFieldWrite[T](implicit builder: SchemaBuilder[T]): SchemaBuilder[Seq[T]] = new SchemaBuilder[Seq[T]] {
    override def schema: Option[Schema] = builder.schema.map { schema => Schema.createArray(schema) }
  }

  implicit def EitherFieldWrite[T, U](implicit twrite: SchemaBuilder[T],
                                      uwrite: SchemaBuilder[U]): SchemaBuilder[Either[T, U]] = new SchemaBuilder[Either[T, U]] {
    override def schema: Option[Schema] = {
      for (t <- twrite.schema;
           u <- uwrite.schema) yield {
        Schema.createUnion(util.Arrays.asList(t, u))
      }
    }
  }

  implicit def RecordFieldWrite[T](implicit builder: RecordSchemaBuilder[T]) = new SchemaBuilder[T] {
    override def schema: Option[Schema] = Some(builder())
  }
}

trait RecordSchemaFields[L <: HList] extends DepFn0 with Serializable {
  type Out = List[Schema.Field]
}

object RecordSchemaFields {

  implicit object HNilFields extends RecordSchemaFields[HNil] {
    def apply() = List.empty
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       builder: SchemaBuilder[V],
                                                       remaining: RecordSchemaFields[T],
                                                       tag: ClassTag[V]): RecordSchemaFields[FieldType[K, V] :: T] = {
    new RecordSchemaFields[FieldType[K, V] :: T] {
      def apply: List[Schema.Field] = {
        val fieldFn: (Schema => Schema.Field) = schema => new Schema.Field(key.value.name, schema, null, null)
        builder.schema.map(fieldFn).toList ++ remaining()
      }
    }
  }
}

trait RecordSchemaBuilder[T] {
  def apply(): Schema
}

object RecordSchemaBuilder {

  implicit def schemaBuilder[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                               schemaFields: RecordSchemaFields[Repr],
                                               tag: ClassTag[T]): RecordSchemaBuilder[T] = new RecordSchemaBuilder[T] {

    import scala.collection.JavaConverters._

    def apply(): Schema = {
      val schema = org.apache.avro.Schema.createRecord(
        tag.runtimeClass.getSimpleName.takeWhile(_ != '$'),
        null,
        tag.runtimeClass.getPackage.getName,
        false
      )
      schema.setFields(schemaFields().asJava)
      schema
    }
  }
}

object AvroSchema2 {
  def apply[T](implicit builder: RecordSchemaBuilder[T]): Schema = builder.apply()
}