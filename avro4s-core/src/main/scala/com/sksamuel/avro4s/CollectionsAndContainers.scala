//
//  implicit def mapSchemaFor[T](implicit value: SchemaFor[T]): SchemaFor[Map[String, T]] =
//    new ResolvableSchemaFor[Map[String, T]] {
//      def schemaFor(env: DefinitionEnvironment[SchemaFor], update: SchemaUpdate): SchemaFor[Map[String, T]] =
//        buildMapSchemaFor(value.resolveSchemaFor(env, update))
//    }
//}
//
//  implicit def mapEncoder[T](implicit value: Encoder[T]): Encoder[Map[String, T]] =
//    new ResolvableEncoder[Map[String, T]] {
//      def encoder(env: DefinitionEnvironment[Encoder], update: SchemaUpdate): Encoder[Map[String, T]] = {
//        val encoder = value.resolveEncoder(env, mapFullUpdate(extractMapValueSchema, update))
//
//        new Encoder[Map[String, T]] {
//          val schemaFor: SchemaFor[Map[String, T]] = buildMapSchemaFor(encoder.schemaFor)
//
//          def encode(value: Map[String, T]): AnyRef = {
//            val map = new util.HashMap[String, AnyRef]
//            value.foreach { case (k, v) => map.put(k, encoder.encode(v)) }
//            map
//          }
//
//          override def withSchema(schemaFor: SchemaFor[Map[String, T]]): Encoder[Map[String, T]] =
//            buildWithSchema(mapEncoder(value), schemaFor)
//        }
//      }
//    }
//}
//
//
//
//  implicit def mapDecoder[T](implicit value: Decoder[T]): Decoder[Map[String, T]] =
//    new ResolvableDecoder[Map[String, T]] {
//      def decoder(env: DefinitionEnvironment[Decoder], update: SchemaUpdate): Decoder[Map[String, T]] = {
//        val decoder = value.resolveDecoder(env, mapFullUpdate(extractMapValueSchema, update))
//
//        new Decoder[Map[String, T]] {
//          val schemaFor: SchemaFor[Map[String, T]] = buildMapSchemaFor(decoder.schemaFor)
//
//          def decode(value: Any): Map[String, T] = value match {
//            case map: java.util.Map[_, _] => map.asScala.toMap.map { case (k, v) => k.toString -> decoder.decode(v) }
//          }
//
//          override def withSchema(schemaFor: SchemaFor[Map[String, T]]): Decoder[Map[String, T]] =
//            buildWithSchema(mapDecoder(value), schemaFor)
//        }
//      }
//    }
//}
//
//object CollectionsAndContainers {
//
//  val noneSchemaFor: SchemaFor[None.type] =
//    SchemaFor(SchemaBuilder.builder.nullType)
//
//  private[avro4s] def buildOptionSchemaFor[T](schemaFor: SchemaFor[T]): SchemaFor[Option[T]] =
//    schemaFor.map[Option[T]](itemSchema => SchemaHelper.createSafeUnion(itemSchema, SchemaBuilder.builder().nullType()))
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
