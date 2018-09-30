package com.sksamuel.avro4s

import scala.annotation.StaticAnnotation

case class AvroAlias(alias: String) extends StaticAnnotation

case class AvroDoc(doc: String) extends StaticAnnotation

case class AvroFixed(size: Int) extends StaticAnnotation

case class AvroName(name: String) extends StaticAnnotation

case class AvroNamespace(namespace: String) extends StaticAnnotation

case class AvroProp(name: String, value: String) extends StaticAnnotation

/**
  * This annotation is used to disable generics in the encoding
  * of a record's name.
  *
  * Normally, the record name for a generic type is the name of the
  * raw type, plus the actual type parameters. For example, a class Foo
  * with type parameters Int and Boolean, would have a generated name of
  * `Foo__Int_Boolean`
  *
  * When this annotation is present on a type, the name used in the
  * schema will simply be the raw type, eg `Foo`.
  */
class AvroErasedName extends StaticAnnotation
