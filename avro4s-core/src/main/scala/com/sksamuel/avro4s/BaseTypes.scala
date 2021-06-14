//
//  implicit def javaEnumSchemaFor[E <: Enum[_]](implicit tag: ClassTag[E]): SchemaFor[E] = {
//    val typeInfo = TypeInfo.fromClass(tag.runtimeClass)
//    val nameExtractor = NameExtractor(typeInfo)
//    val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)
//
//    val maybeName = tag.runtimeClass.getAnnotations.collectFirst {
//      case annotation: AvroJavaName => annotation.value()
//    }
//
//    val maybeNamespace = tag.runtimeClass.getAnnotations.collectFirst {
//      case annotation: AvroJavaNamespace => annotation.value()
//    }
//
//    val name = maybeName.getOrElse(nameExtractor.name)
//    val namespace = maybeNamespace.getOrElse(nameExtractor.namespace)
//
//    val maybeEnumDefault = tag.runtimeClass.getDeclaredFields.collectFirst {
//      case field if field.getDeclaredAnnotations.map(_.annotationType()).contains(classOf[AvroJavaEnumDefault]) =>
//        field.getName
//    }
//
//    val schema = maybeEnumDefault
//      .map { enumDefault =>
//        SchemaBuilder.enumeration(name).namespace(namespace).defaultSymbol(enumDefault).symbols(symbols: _*)
//      }
//      .getOrElse {
//        SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
//      }
//
//    val props = tag.runtimeClass.getAnnotations.collect {
//      case annotation: AvroJavaProp => annotation.key() -> annotation.value()
//    }
//
//    props.foreach {
//      case (key, value) =>
//        schema.addProp(key, value)
//    }
//    SchemaFor[E](schema)
//  }
//
//  def getAnnotationValue[T](annotationClass: Class[T], annotations: Seq[Annotation]): Option[String] = {
//    annotations.collectFirst {
//      case a: Annotation if a.tree.tpe.typeSymbol.name.toString == annotationClass.getSimpleName =>
//        a.tree.children.tail.headOption.flatMap {
//          case select: Select => Some(select.name.toString)
//          case _              => None
//        }
//    }.flatten
//  }
//
//
//  implicit def javaEnumEncoder[E <: Enum[E]: ClassTag]: JavaEnumEncoder[E] = new JavaEnumEncoder[E]
//
//  implicit def scalaEnumEncoder[E <: Enumeration#Value: TypeTag]: ScalaEnumEncoder[E] = new ScalaEnumEncoder[E]
//
//  class JavaEnumEncoder[E <: Enum[E]](implicit tag: ClassTag[E]) extends Encoder[E] {
//    val schemaFor: SchemaFor[E] = SchemaFor.javaEnumSchemaFor[E]
//    def encode(value: E): AnyRef = new EnumSymbol(schema, value.name)
//  }
//
//  class ScalaEnumEncoder[E <: Enumeration#Value](implicit tag: TypeTag[E]) extends Encoder[E] {
//    val schemaFor: SchemaFor[E] = SchemaFor.scalaEnumSchemaFor[E]
//    def encode(value: E): AnyRef = new EnumSymbol(schema, value.toString)
//  }
//}
//
//
//  implicit val UUIDDecoder: Decoder[UUID] = StringDecoder.map[UUID](UUID.fromString).withSchema(SchemaFor.UUIDSchemaFor)
//
//  implicit def javaEnumDecoder[E <: Enum[E]: ClassTag]: JavaEnumDecoder[E] = new JavaEnumDecoder[E]
//
//  implicit def scalaEnumDecoder[E <: Enumeration#Value: TypeTag]: ScalaEnumDecoder[E] = new ScalaEnumDecoder[E]
//
//  class JavaEnumDecoder[E <: Enum[E]](implicit tag: ClassTag[E]) extends Decoder[E] {
//    val schemaFor: SchemaFor[E] = SchemaFor.javaEnumSchemaFor[E]
//    def decode(value: Any): E = Enum.valueOf(tag.runtimeClass.asInstanceOf[Class[E]], value.toString)
//  }
//
//  class ScalaEnumDecoder[E <: Enumeration#Value](implicit tag: TypeTag[E]) extends Decoder[E] {
//    val enum = tag.tpe match {
//      case TypeRef(enumType, _, _) =>
//        val moduleSymbol = enumType.termSymbol.asModule
//        val mirror: Mirror = runtimeMirror(getClass.getClassLoader)
//        mirror.reflectModule(moduleSymbol).instance.asInstanceOf[Enumeration]
//    }
//
//    val schemaFor: SchemaFor[E] = SchemaFor.scalaEnumSchemaFor[E]
//
//    def decode(value: Any): E = enum.withName(value.toString).asInstanceOf[E]
//  }
//}
