package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData}
import org.apache.avro.util.Utf8
import scala.collection.JavaConverters._

protected abstract class SafeFrom[T: Decoder] {
  val decoder: Decoder[T] = implicitly[Decoder[T]]
  def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T]
}

object SafeFrom {

  import scala.reflect.runtime.universe._

  def makeSafeFrom[T: Decoder : WeakTypeTag : Manifest]: SafeFrom[T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case _: Utf8 => Some(decoder.decode(value, schema, fieldMapper))
            case _: String => Some(decoder.decode(value, schema, fieldMapper))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Boolean]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case true | false => Some(decoder.decode(value, schema, fieldMapper))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Int]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case _: Int => Some(decoder.decode(value, schema, fieldMapper))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Long]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case _: Long => Some(decoder.decode(value, schema, fieldMapper))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Double]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case _: Double => Some(decoder.decode(value, schema, fieldMapper))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Float]) {
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case _: Float => Some(decoder.decode(value, schema, fieldMapper))
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[Array[_]] ||
      tpe <:< typeOf[java.util.Collection[_]] ||
      tpe <:< typeOf[Iterable[_]]) {

      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case _: GenericData.Array[_] =>
              trySchemas(value, fieldMapper, decoder) {
                possibleSchemas(schema).filter(_.getType == Schema.Type.ARRAY)
              }
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[java.util.Map[_, _]] ||
      tpe <:< typeOf[Map[_, _]]) {

      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case _: java.util.Map[_, _] =>
              trySchemas(value, fieldMapper, decoder) {
                possibleSchemas(schema).filter(_.getType == Schema.Type.MAP)
              }
            case _ => None
          }
        }
      }
    } else if (tpe <:< typeOf[shapeless.Coproduct]) {
      // this is only sort of safe because we will call SafeFrom again in the decoder
      new SafeFrom[T] {
        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          util.Try(decoder.decode(value, schema, fieldMapper)) match {
            case util.Success(cp) => Some(cp)
            case _ => None
          }
        }
      }
    } else {
      new SafeFrom[T] {

        private val nameExtractor = NameExtractor(manifest.runtimeClass)
        private val typeName = nameExtractor.fullName

        override def safeFrom(value: Any, schema: Schema, fieldMapper: FieldMapper): Option[T] = {
          value match {
            case container: GenericContainer if typeName == container.getSchema.getFullName =>
              val s = schema.getType match {
                case Schema.Type.UNION => SchemaHelper.extractTraitSubschema(nameExtractor.fullName, schema)
                case _ => schema
              }
              Some(decoder.decode(value, s, fieldMapper))
            case _ => None
          }
        }
      }
    }
  }

  private def possibleSchemas(schema: Schema): List[Schema] =
    schema.getType match {
      case Schema.Type.UNION => schema.getTypes.asScala.toList
      case _ => List(schema)
    }

  private def trySchemas[T](value: Any, fieldMapper: FieldMapper, decoder: Decoder[T])(schemas: List[Schema]): Option[T] = {
    @annotation.tailrec
    def go(schemas: List[Schema], failed: List[(Schema, Throwable)]): Option[T] =
      schemas match {
        case Nil =>
          val msg = failed.reverse.map { case (s, error) =>
            s"Type $s with ${error.getMessage}"
          }.mkString("\n")
          sys.error(s"Failed to decode:\n$msg")
        case hd :: tl =>
          util.Try(decoder.decode(value, hd, fieldMapper)).toEither match {
            case Right(t) => Some(t)
            case Left(error) => go(tl, (hd, error) :: failed)
          }
      }

    go(schemas, Nil)
  }
}
