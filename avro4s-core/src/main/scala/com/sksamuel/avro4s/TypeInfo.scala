////
////object TypeInfo {
////  def fromTypeName(typeName: TypeName): TypeInfo = {
////    // try to populate from the class name, but this may fail if the class is not top level
////    // if it does fail then we default back to using what magnolia provides
////    val maybeType: Option[universe.Type] = Try {
////      val mirror = universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
////      val classsym = mirror.staticClass(typeName.full)
////      classsym.toType
////    }.toOption
////
////    TypeInfo(
////      owner = typeName.owner,
////      short = typeName.short,
////      typeArguments = typeName.typeArguments.map(fromTypeName),
////      nameAnnotation = maybeType.flatMap(nameAnnotation),
////      namespaceAnnotation = maybeType.flatMap(namespaceAnnotation),
////      erased = maybeType.exists(erased)
////    )
////  }
////
////  def fromClass[A](klass: Class[A]): TypeInfo = {
////    import scala.reflect.runtime.universe
////    val mirror = universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
////    val sym = mirror.classSymbol(klass)
////    val tpe = sym.toType
////    TypeInfo.fromType(tpe)
////  }
////
////
////  def fromType(tpe: universe.Type): TypeInfo = {
////    TypeInfo(
////      tpe.typeSymbol.owner.fullName,
////      tpe.typeSymbol.name.decodedName.toString,
////      tpe.typeArgs.map(fromType),
////      nameAnnotation(tpe),
////      namespaceAnnotation(tpe),
////      erased(tpe)
////    )
////  }
////}