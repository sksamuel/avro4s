//package com.sksamuel.avro4s
//
//import com.sksamuel.avro4s.SchemaUpdate.{FullSchemaUpdate, NamespaceUpdate, NoUpdate}
//import com.sksamuel.avro4s.TypeUnionEntry._
//import com.sksamuel.avro4s.TypeUnions._
//import magnolia.{SealedTrait, Subtype}
//import org.apache.avro.Schema
//import org.apache.avro.generic.GenericContainer
//
//class TypeUnionEncoder[T](ctx: SealedTrait[Encoder, T],
//                          val schemaFor: SchemaFor[T],
//                          encoderBySubtype: Map[Subtype[Encoder, T], UnionEncoder[T]#SubtypeEncoder])
//    extends Encoder[T] {
//
//  override def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
//    validateNewSchema(schemaFor)
//    TypeUnions.encoder(ctx, new DefinitionEnvironment[Encoder](), FullSchemaUpdate(schemaFor))
//  }
//
//  def encode(value: T): AnyRef =
//    // we need an additional indirection since we may have enhanced the original magnolia-provided encoder via annotations
//    ctx.dispatch(value)(subtype => encoderBySubtype(subtype).encodeSubtype(value))
//}
//
//class TypeUnionDecoder[T](ctx: SealedTrait[Decoder, T],
//                          val schemaFor: SchemaFor[T],
//                          decoderByName: Map[String, UnionDecoder[T]#SubtypeDecoder])
//    extends Decoder[T] {
//
//  override def withSchema(schemaFor: SchemaFor[T]): Decoder[T] = {
//    validateNewSchema(schemaFor)
//    TypeUnions.decoder(ctx, new DefinitionEnvironment[Decoder](), FullSchemaUpdate(schemaFor))
//  }
//
//  def decode(value: Any): T = value match {
//    case container: GenericContainer =>
//      val schemaName = container.getSchema.getFullName
//      val codecOpt = decoderByName.get(schemaName)
//      if (codecOpt.isDefined) {
//        codecOpt.get.decodeSubtype(container)
//      } else {
//        val schemaNames = decoderByName.keys.toSeq.sorted.mkString("[", ", ", "]")
//        throw new Avro4sDecodingException(s"Could not find schema $schemaName in type union schemas $schemaNames", value, this)
//      }
//    case _ => throw new Avro4sDecodingException(s"Unsupported type $value in type union decoder", value, this)
//  }
//}
//
