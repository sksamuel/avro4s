package com.sksamuel.avro4s

import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.record.Keys
import shapeless.tag.{Tagged, @@}

object recordKeys extends Poly1 {
  implicit def caseAll[K, V] = at[FieldType[K, V]](m => println(m))
}

object fieldsToSchemas extends Poly1 {
  implicit def string = at[String](str => println(str))
}

object bind extends Poly1 {
  implicit def default[E] = at[E](identity)
  implicit def caseString[Unit] = at[FieldType[Unit, String]](str => println(str))
}

trait FieldMapper[T] {
  def field(name: String, value: T): Option[Schema.Field]
}

object FieldMapper {

  implicit object StringFieldMapper extends FieldMapper[String] {
    override def field(name: String, value: String): Option[Field] = {
      Some(new Schema.Field(name, Schema.create(Schema.Type.STRING), null, null))
    }
  }

  implicit object HNilFieldMapper extends FieldMapper[HNil] {
    override def field(name: String, value: HNil): Option[Schema.Field] = None
  }

  implicit def HConsFieldMapper[H: FieldMapper, T <: HList : FieldMapper]: FieldMapper[H :: T] = {
    new FieldMapper[H :: T] {
      override def field(name: String, value: H :: T): Option[Schema.Field] = value match {
        case h :: t =>
          val sh = implicitly[H](h)
          val st = implicitly[T](t)
          None
      }
    }
  }

  implicit def GenericFieldMapper[T, Repr <: HList](implicit rmapper: FieldMapper[Repr]) = new FieldMapper[Repr] {
    override def field(name: String, value: Repr): Option[Schema.Field] = None
  }
}

trait RecordBuilder[T] {
  def apply(): Schema
}

object RecordBuilder {

  implicit def recordBuilder[T, Repr <: HList](implicit label: LabelledGeneric.Aux[T, Repr]): RecordBuilder[T] = new RecordBuilder[T] {

    def apply(): Schema = {
      val schema = org.apache.avro.Schema.createRecord("todo", null, "com.todo", false)
      schema.setFields(util.Arrays.asList(new Schema.Field("abc", Schema.create(Schema.Type.BOOLEAN), null, null)))
      schema
    }
  }
}

object SchemaGen {
  def apply[T](implicit recordBuilder: RecordBuilder[T]): Schema = recordBuilder()
}

case class Foo(bar: String, buzz: Boolean)

object MyFunc extends Poly1 {

  import scala.reflect.runtime.universe.TypeTag
  import scala.reflect.runtime.universe.typeTag

  implicit def all[A: TypeTag] = at[Symbol with Tagged[A]](x => typeTag[A].tpe match {
    case t if t <:< typeTag[String].tpe => t + " is a string"
    case t if t <:< typeTag[Boolean].tpe => t + " is a boolean"
    case t if t <:< typeTag[Int].tpe => x.name + " is an int"
  })
}

//object Test extends App {
//
//  val labl = LabelledGeneric[Foo]
//  val keys = Keys[labl.Repr].apply
//
//  val things = keys.map(MyFunc)
//  println("things=" + things)
//}
