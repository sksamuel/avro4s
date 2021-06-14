package com.sksamuel.avro4s

import scala.annotation.StaticAnnotation

case class AvroAlias(override val alias: String) extends AvroAliasable

trait AvroAliasable extends AvroFieldReflection {
  val alias: String
}

case class AvroNoDefault() extends StaticAnnotation

case class AvroDoc(override val doc: String) extends AvroDocumentable

trait AvroDocumentable extends AvroFieldReflection {
  val doc: String
}

/**
  * [[AvroFixed]] overrides the schema type for a field or a value class
  * so that the schema is set to org.apache.avro.Schema.Type.FIXED
  * rather than whatever the default would be.
  *
  * This annotation can be used in the following ways:
  *
  * - On a field, eg case class `Foo(@AvroField(10) name: String)`
  * which results in the field `name` having schema type FIXED with
  * a size of 10.
  *
  * - On a value type, eg `@AvroField(7) case class Foo(name: String) extends AnyVal`
  * which results in all usages of the value type having schema
  * FIXED with a size of 7 rather than the default.
  */
case class AvroFixed(override val size: Int) extends AvroFixable

trait AvroFixable extends AvroFieldReflection {
  val size: Int
}

/**
  * [[AvroName]] allows the name used by Avro to be different
  * from what is defined in code.
  *
  * For example, if a case class defines a field z, such as
  * `case class Foo(z: String)` then normally this will be
  * serialized as an entry 'z' in the Avro Record.
  *
  * However, if the field is annotated such as
  * `case class Foo(@AvroName("x") z: String)` then the entry
  * in the Avro Record will be for 'x'.
  *
  * Similarly for deserialization, if a field is annotated then
  * the name that is looked up in the avro record will be the
  * annotated name and not the field name in Scala.
  *
  * The second example is with classes. If a class is annotated
  * with @AvroName then the name used in the record schema
  * will not be the classname but the annotated value.
  *
  * This will also have an effect on serialization.
  * For example, when decoding records into an Either, the
  * decoder must decide if the value is a Left or a Right.
  * It usually does this by comparing the name in the record
  * to the classnames of the either types, but when annotated,
  * it will compare the name in the record to the annotated value.
  *
  */
case class AvroName(override val name: String) extends AvroNameable

case class AvroTransient() extends StaticAnnotation

trait AvroNameable extends AvroFieldReflection {
  val name: String
}

case class AvroNamespace(override val namespace: String) extends AvroNamespaceable

trait AvroNamespaceable extends AvroFieldReflection {
  val namespace: String
}

case class AvroProp(override val key: String, override val value:String) extends AvroProperty

trait AvroProperty extends AvroFieldReflection {
  val key: String
  val value: String
}

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
case class AvroErasedName() extends AvroFieldReflection


sealed trait AvroFieldReflection extends StaticAnnotation {
  private def getClassFields(clazz: Class[_]): Map[String, Any] = {
    val fields = clazz.getDeclaredFields.map(field => {
      field setAccessible true
      field.getName -> field.get(this)
    }).toMap
    if(clazz.getSuperclass != null){
      fields ++ getClassFields(clazz.getSuperclass)
    }
    fields
  }

  def getAllFields = getClassFields(this.getClass)
}

case class AvroSortPriority(priority: Float) extends AvroFieldReflection

case class AvroEnumDefault(default: Any) extends StaticAnnotation

case class AvroUnionPosition(position: Int) extends StaticAnnotation

/**
  * Use this annotation to indicate that a record type should be generated as an avro error type.
  */
case class AvroError() extends StaticAnnotation

case class AvroScalePrecision(scale: Int, precision: Int) extends StaticAnnotation