//
//  private[avro4s] def extractOptionSchema(schema: Schema): Schema = {
//    if (schema.getType != Schema.Type.UNION)
//      throw new Avro4sConfigurationException(
//        s"Schema type for option encoders / decoders must be UNION, received $schema")
//
//    val schemas = schema.getTypes.asScala.filterNot(_.getType == Schema.Type.NULL)
//
//    schemas.size match {
//      case 0 => throw new Avro4sConfigurationException(s"Union schema $schema doesn't contain any non-null entries")
//      case 1 => schemas.head
//      case _ => Schema.createUnion(schemas.asJava)
//    }
//  }
//
//  private[avro4s] def buildEitherSchemaFor[A, B](leftSchemaFor: SchemaFor[A],
//                                                 rightSchemaFor: SchemaFor[B]): SchemaFor[Either[A, B]] =
//    SchemaFor(SchemaHelper.createSafeUnion(leftSchemaFor.schema, rightSchemaFor.schema), leftSchemaFor.fieldMapper)
//

//

//
//  private[avro4s] def buildIterableSchemaFor[C[X] <: Iterable[X], T](schemaFor: SchemaFor[T]): SchemaFor[C[T]] =
//    schemaFor.map(SchemaBuilder.array.items(_))
//
//  private[avro4s] def extractIterableElementSchema(schema: Schema): Schema = {
//    if (schema.getType != Schema.Type.ARRAY)
//      throw new Avro4sConfigurationException(
//        s"Schema type for array / list / seq / vector encoders and decoders must be ARRAY, received $schema")
//    schema.getElementType
//  }
//
//  private[avro4s] def buildMapSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[Map[String, T]] =
//    schemaFor.map(SchemaBuilder.map().values(_))
//
//  private[avro4s] def extractMapValueSchema(schema: Schema): Schema = {
//    if (schema.getType != Schema.Type.MAP)
//      throw new Avro4sConfigurationException(s"Schema type for map encoders / decoders must be MAP, received $schema")
//    schema.getValueType
//  }
//}
