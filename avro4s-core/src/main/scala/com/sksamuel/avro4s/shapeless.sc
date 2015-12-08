
import java.util
import com.sksamuel.avro4s.SchemaGen
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import shapeless.labelled.FieldType
import shapeless.ops.record.{Values, Keys}
import scala.language.implicitConversions
import shapeless._
import shapeless.record._
import shapeless.syntax.singleton._

//
//trait SchemaGenerator[T] {
//  def schema(name: String, value: T, schema: Schema): Unit
//}
//
//
//implicit object StringSchemaGenerator extends SchemaGenerator[String] {
//  override def schema(name: String, value: String, schema: Schema): Unit = {
//    val field = new Field(name, Schema.create(Schema.Type.STRING), null, null)
//    schema.getFields.add(field)
//  }
//}
//
//implicit object HNilSchemaGenerator extends SchemaGenerator[HNil] {
//  override def schema(name: String, value: HNil, schema: Schema): Unit = ()
//}
//
//implicit def HConsSchemaGenerator[H: SchemaGenerator, T <: HList : SchemaGenerator]: SchemaGenerator[H :: T] = {
//  new SchemaGenerator[H :: T] {
//    override def schema(name: String, value: H :: T, schema: Schema): Unit = value match {
//      case h :: t =>
//        val sh = implicitly[H](h)
//        val st = implicitly[T](t)
//    }
//  }
//}
//
//implicit def GenericSchemaGenerator[T, Repr <: HList](implicit label: LabelledGeneric.Aux[T, Repr],
//                                                      vv: Values[Repr],
//                                                      kk: Keys[Repr]): Schema = {
//  val schema = org.apache.avro.Schema.createRecord("todo", null, "com.todo", false)
//  schema.setFields(util.Arrays.asList(new Schema.Field("abc", Schema.create(Schema.Type.BOOLEAN), null, null)))
//  schema
//}







//val v = toNamedSingletonListOfValues(r)
//println("values=" + v)
