package com.sksamuel.avro4s.internal

import java.io.Serializable

import org.apache.avro.Schema

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

trait SchemaEncoder[A] extends Serializable {
  self =>

  def encode(a: A): Schema

  /**
    * Create a new [[SchemaEncoder]] by applying a function to a value of type `B`
    * before encoding as an `A`.
    */
  final def contramap[B](f: B => A): SchemaEncoder[B] = new SchemaEncoder[B] {
    final def encode(b: B): Schema = self.encode(f(b))
  }

  /**
    * Create a new [[SchemaEncoder]] by applying a function to the output of this one.
    */
  final def map(f: Schema => Schema): SchemaEncoder[A] = new SchemaEncoder[A] {
    final def encode(a: A): Schema = f(self.encode(a))
  }
}

object SchemaMacros {

  implicit def apply[T]: StructType = macro SchemaMacros.applyImpl[T]

  def applyImpl[T: c.WeakTypeTag](c: whitebox.Context): c.Expr[StructType] = {

    import c.universe._

    val reflect = ReflectHelper(c)

    implicit val stringTypeLift: c.universe.Liftable[StringType.type] = Liftable[StringType.type ] { _ =>
      q"""_root_.com.sksamuel.avro4s.internal.StringType"""
    }

    /**
      * Returns the [[DataType]] for the given type.
      * This may be a complex type, in which case a [[StructType]] will be returned.
      * Otherwise it will return one of the primitive types.
      */
    def dataType(tpe: Type): c.Expr[StructType] = {
      if (tpe.typeSymbol.isClass) { println(tpe) }
      c.Expr[StructType](q"""{ _root_.com.sksamuel.avro4s.internal.StringType }""")
    }

    def structType(tpe: Type): c.Expr[StructType] = {

      val underlyingType = reflect.underlyingType(tpe)

      // qualified name of the clsas we are building
      // todo encode generics:: + genericNameSuffix(underlyingType)
      val className = underlyingType.typeSymbol.fullName.toString

      // these are annotations on the class itself
      val annos = reflect.annotations(underlyingType.typeSymbol)

      val fields = reflect.fieldsOf(underlyingType).zipWithIndex.map { case ((f, fieldTpe), index) =>

        // the simple name of the field like "x"
        val fieldName = f.name.decodedName.toString.trim
        val annos = reflect.annotations(f)
        val dt = dataType(fieldTpe)

        q"""{ _root_.com.sksamuel.avro4s.internal.StructField($fieldName, $dt, Seq(..$annos), null) }"""
      }

      c.Expr[StructType](q"""{ _root_.com.sksamuel.avro4s.internal.StructType($className, Seq(..$annos), Seq(..$fields)) }""")
    }

    val tType = weakTypeOf[T]

    // we can only encode concrete classes as the top level
    require(tType.typeSymbol.isClass, tType + " is not a class but is " + tType.typeSymbol.fullName)
    structType(tType)
  }
}