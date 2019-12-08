import sbt._

/*
 * Generate a range of boilerplate classes that would be tedious to write and maintain by hand.
 *
 * Copied, with some modifications, from
 * [[https://github.com/milessabin/shapeless/blob/master/project/Boilerplate.scala Shapeless]].
 */
object Boilerplate {
  import scala.StringContext._

  implicit class BlockHelper(val sc: StringContext) extends AnyVal {

    def block(args: Any*): String = {
      val interpolated = sc.standardInterpolator(treatEscapes, args)
      val rawLines = interpolated.split('\n')
      val trimmedLines = rawLines.map { _.dropWhile(_.isWhitespace) }
      trimmedLines.mkString("\n")
    }
  }

  val templates: Seq[Template] = List(
    GenSchemaCodecProductTypes
  )

  /** Returns a seq of the generated files.  As a side-effect, it actually generates them... */
  def gen(dir: File) = for (t <- templates) yield {
    val tgtFile = dir / "com" / "sksamuel" / "avro4s" / t.filename
    IO.write(tgtFile, t.body)
    tgtFile
  }

  val header = "// Auto-generated boilerplate. See `project/Boilerplate.scala`"

  class TemplateVals(val arity: Int) {
    val synTypes = (0 until arity).map(n => (n + 'A').toChar)

    val `A..N` = synTypes.mkString(", ")
  }

  trait Template {
    def filename: String
    def content(tv: TemplateVals): String
    def range = 1 to 22

    def body: String = {
      val headerLines = header.split('\n')
      val rawContents = range.map { n =>
        content(new TemplateVals(n)).split('\n').filterNot(_.isEmpty)
      }
      val preBody = rawContents.head.takeWhile(_.startsWith("|")).map(_.tail)
      val instances = rawContents.flatMap { _.filter(_.startsWith("-")).map(_.tail) }
      val postBody =
        rawContents.head.dropWhile(_.startsWith("|")).dropWhile(_.startsWith("-")).map(_.tail)
      (headerLines ++ preBody ++ instances ++ postBody).mkString("\n")
    }
  }

  /*
    Blocks in the templates below use a custom interpolator, combined with post-processing to produce the body
      - The contents of the `header` val is output first
      - Then the first block of lines beginning with '|'
      - Then the block of lines beginning with '-' is replicated once for each arity,
        with the `templateVals` already pre-populated with relevant relevant vals for that arity
      - Then the last block of lines prefixed with '|'
    The block otherwise behaves as a standard interpolated string with regards to variable substitution.
   */

  object GenSchemaCodecProductTypes extends Template {
    val filename = "SchemaCodecProductTypes.scala"

    def content(tv: TemplateVals) = {
      import tv._

      val instances = synTypes.map(tpe => s"schemaCodec$tpe: SchemaCodec[$tpe]").mkString(", ")
      val names = synTypes.map(tpe => s"name$tpe: String").mkString(", ")
      val recordFieldChainCalls = synTypes
        .map(tpe =>
          s".name(name$tpe).`type`(schemaCodec$tpe.schemaFor.schema(DefaultFieldMapper)).noDefault()"
        )
        .mkString("")
      val gs = synTypes.map(tpe => s"g$tpe").mkString(", ")
      val encoderVectorArgs = synTypes
        .map(tpe =>
          s"schemaCodec$tpe.encoder.encode(g$tpe, schema.getField(name$tpe).schema, fieldMapper)"
        )
        .mkString(",")
      val decoderFArgs = synTypes
        .map(tpe =>
          s"schemaCodec$tpe.decoder.decode(record.get(name$tpe), schema.getField(name$tpe).schema(), fieldMapper)"
        )
        .mkString(",")

      block"""
      |package com.sksamuel.avro4s
      |
      |import org.apache.avro.generic.GenericRecord
      |import org.apache.avro.{Schema, SchemaBuilder}
      |
      |trait SchemaCodecProductTypes {
      |
      -  def forProduct${arity}[Target, ${`A..N`}](namespace: String, recordName: String)($names)(
      -    f: (${`A..N`}) => Target
      -  )(g: Target => (${`A..N`}))(implicit $instances): SchemaCodec[Target] = {
      -    val targetSchema = SchemaBuilder
      -      .record(recordName)
      -      .namespace(namespace)
      -      .fields()
      -        ${recordFieldChainCalls}
      -      .endRecord()
      -
      -    new SchemaCodec[Target] {
      -      override def schemaFor: SchemaFor[Target] = new SchemaFor[Target] {
      -        override def schema(fieldMapper: FieldMapper): Schema = targetSchema
      -      }
      -
      -      override def encoder: Encoder[Target] = new Encoder[Target] {
      -        override def encode(t: Target, schema: Schema, fieldMapper: FieldMapper): AnyRef = {
      -          val ($gs) = g(t)
      -          ImmutableRecord(
      -            schema,
      -            Vector(
      -              ${encoderVectorArgs}
      -            )
      -          )
      -        }
      -      }
      -
      -      override def decoder: Decoder[Target] = new Decoder[Target] {
      -        override def decode(t: Any, schema: Schema, fieldMapper: FieldMapper): Target = {
      -          val record = t.asInstanceOf[GenericRecord]
      -          f(${decoderFArgs})
      -        }
      -      }
      -    }
      -  }
      |}
      """
    }
  }
}
