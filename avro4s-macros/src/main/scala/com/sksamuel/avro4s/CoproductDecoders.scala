package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData}
import org.apache.avro.util.Utf8
import shapeless.{:+:, CNil, Coproduct, Generic, Inr}

trait CoproductDecoders  {

  import scala.reflect.runtime.universe._

  implicit def genCoproductDecoder[T, C <: Coproduct](implicit gen: Generic.Aux[T, C],
                                                      decoder: Decoder[C]): Decoder[T] = new Decoder[T] {
    override def decode(value: Any, schema: Schema): T = {
      gen.from(decoder.decode(value, schema))
    }
  }

  // A coproduct is a union, or a generalised either.
  // A :+: B :+: C :+: CNil is a type that is either an A, or a B, or a C.

  // Shapeless's implementation builds up the type recursively,
  // (i.e., it's actually A :+: (B :+: (C :+: CNil)))

  // `decode` here should never be invoked under normal operation; if
  // we're trying to read a value of type CNil it's because we've
  // tried all the other cases and failed. But the Decoder[CNil]
  // needs to exist to supply a base case for the recursion.
  implicit object CNilDecoderValue extends Decoder[CNil] {
    override def decode(value: Any, schema: Schema): CNil = sys.error("This should never happen: CNil has no inhabitants")
  }

  // We're expecting to read a value of type S :+: T from avro.  Avro
  // unions are untyped, so we have to attempt to read a value of type
  // S (the concrete type), and if that fails, attempt to read the
  // rest of the coproduct type T.

  // thus, the bulk of the logic here is shared with reading Eithers, in `safeFrom`.
  implicit def coproductDecoder[S: WeakTypeTag : Decoder, T <: Coproduct](implicit decoder: Decoder[T]): Decoder[S :+: T] = new Decoder[S :+: T] {
    override def decode(value: Any, schema: Schema): S :+: T = {
      safeFrom[S](value, schema) match {
        case Some(s) => Coproduct[S :+: T](s)
        case None => Inr(decoder.decode(value, schema))
      }
    }
  }

  protected def safeFrom[T: WeakTypeTag](value: Any, schema: Schema)(implicit decoder: Decoder[T]): Option[T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    def typeName: String = AvroNamespaceResolver.forClass(tpe) + "." + AvroNameResolver.forClass(tpe)
    //      val nearestPackage = Stream.iterate(tpe.typeSymbol.owner)(_.owner).dropWhile(!_.isPackage).head
    //      s"${nearestPackage.fullName}.${tpe.typeSymbol.name.decodedName}"
    //    }

    value match {
      case _: Utf8 if tpe <:< typeOf[java.lang.String] => Some(decoder.decode(value, schema))
      case _: String if tpe <:< typeOf[java.lang.String] => Some(decoder.decode(value, schema))
      case true | false if tpe <:< typeOf[Boolean] => Some(decoder.decode(value, schema))
      case _: Int if tpe <:< typeOf[Int] => Some(decoder.decode(value, schema))
      case _: Long if tpe <:< typeOf[Long] => Some(decoder.decode(value, schema))
      case _: Double if tpe <:< typeOf[Double] => Some(decoder.decode(value, schema))
      case _: Float if tpe <:< typeOf[Float] => Some(decoder.decode(value, schema))
      // we don't need to worry about the inner type of the array,
      // as avro schemas will not legally allow multiple arrays in a union
      // tpe is the type we're _expecting_, though, so we need to
      // check both scala and java collections
      case _: GenericData.Array[_]
        if tpe <:< typeOf[Array[_]] ||
          tpe <:< typeOf[java.util.Collection[_]] ||
          tpe <:< typeOf[Iterable[_]] =>
        Some(decoder.decode(value, schema))
      // and similarly for maps
      case _: java.util.Map[_, _]
        if tpe <:< typeOf[java.util.Map[_, _]] ||
          tpe <:< typeOf[Map[_, _]] =>
        Some(decoder.decode(value, schema))
      // we compare the name in the record to the type name we supplied, if they match then this is correct type to decode to
      case container: GenericContainer if typeName == container.getSchema.getFullName => Some(decoder.decode(value, schema))
      // if nothing matched then this wasn't the type we expected
      case _ => None
    }
  }
}
