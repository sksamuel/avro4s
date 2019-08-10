package com.sksamuel.avro4s

import magnolia.TypeName

import scala.util.Try

case class TypeInfo(owner: String,
                    short: String,
                    typeArguments: Seq[TypeInfo],
                    nameAnnotation: Option[String],
                    namespaceAnnotation: Option[String],
                    erased: Boolean) {
  val full: String = s"$owner.$short"
}

object TypeInfo {

  import scala.reflect.runtime.universe

  def apply(typeName: TypeName, annos: Seq[Any]): TypeInfo = {
    val annotationExtractors = new AnnotationExtractors(annos)
    TypeInfo(
      typeName.owner, typeName.short, typeName.typeArguments.map(TypeInfo.fromTypeName),
      annotationExtractors.name,
      annotationExtractors.namespace,
      annotationExtractors.erased
    )
  }

  def fromTypeName(typeName: TypeName): TypeInfo = {
    // try to populate from the class name, but this may fail if the class is not top level
    // if it does fail then we default back to using what magnolia provides
    Try {
      val mirror = universe.runtimeMirror(this.getClass.getClassLoader)
      val classsym = mirror.staticClass(typeName.full)
      fromType(classsym.toType)
    }.getOrElse {
      TypeInfo(typeName.owner, typeName.short, typeName.typeArguments.map(fromTypeName), None, None, false)
    }
  }

  def fromClass[A](klass: Class[A]): TypeInfo = {
    import scala.reflect.runtime.universe
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val sym = mirror.classSymbol(klass)
    val tpe = sym.toType
    TypeInfo.fromType(tpe)
  }

  def fromType(tpe: universe.Type): TypeInfo = {
    import scala.reflect.runtime.universe._

    val nameAnnotation = tpe.typeSymbol.typeSignature.typeSymbol.annotations.collectFirst {
      case a if a.tree.tpe =:= typeOf[AvroName] =>
        val annoValue = a.tree.children.tail.head.asInstanceOf[Literal].value.value
        annoValue.toString
    }

    val namespaceAnnnotation = tpe.typeSymbol.typeSignature.typeSymbol.annotations.collectFirst {
      case a if a.tree.tpe =:= typeOf[AvroNamespace] =>
        val annoValue = a.tree.children.tail.head.asInstanceOf[Literal].value.value
        annoValue.toString
    }

    val erased = tpe.typeSymbol.typeSignature.typeSymbol.annotations.exists {
      case a if a.tree.tpe =:= typeOf[AvroErasedName] => true
      case _ => false
    }

    TypeInfo(tpe.typeSymbol.owner.fullName, tpe.typeSymbol.name.decodedName.toString, tpe.typeArgs.map(fromType), nameAnnotation, namespaceAnnnotation, erased)
  }
}