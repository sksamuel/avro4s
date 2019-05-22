package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData}
import org.apache.avro.util.Utf8

protected abstract class SafeFrom[T: Decoder] {
  val decoder: Decoder[T] = implicitly[Decoder[T]]
  def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T]
}

object SafeFrom {

  import scala.reflect.runtime.universe._

  def makeSafeFrom[T: Decoder : WeakTypeTag : Manifest]: SafeFrom[T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case _: Utf8 => Some(decoder.decode(value, schema, naming))
            case _: String => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Boolean]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case true | false => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Int]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case _: Int => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Long]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case _: Long => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Double]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case _: Double => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Float]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case _: Float => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Array[_]] ||
      tpe <:< typeOf[java.util.Collection[_]] ||
      tpe <:< typeOf[Iterable[_]]) {

      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case _: GenericData.Array[_] => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[java.util.Map[_, _]] ||
      tpe <:< typeOf[Map[_, _]]) {

      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case _: java.util.Map[_, _] => Some(decoder.decode(value, schema, naming))
            case _ => None
          }
        }
      }
    } else {
      new SafeFrom[T] {

        private val namer = Namer(manifest.runtimeClass)
        private val typeName = namer.fullName

        override def safeFrom(value: Any, schema: Schema, naming: NamingStrategy): Option[T] = {
          value match {
            case container: GenericContainer if typeName == container.getSchema.getFullName =>
              val s = schema.getType match {
                case Schema.Type.UNION => SchemaHelper.extractTraitSubschema(namer.fullName, schema)
                case _ => schema
              }
              Some(decoder.decode(value, s, naming))
            case _ => None
          }
        }
      }
    }
  }
}
