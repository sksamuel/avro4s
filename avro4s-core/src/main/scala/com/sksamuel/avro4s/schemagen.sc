
import java.util
import com.sksamuel.avro4s.FieldWrite
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.avro.Schema.Field
import shapeless._
import shapeless.syntax._
import shapeless.syntax.singleton._
import shapeless.labelled._

import scala.reflect.ClassTag



case class Foo(bar: String, buzz: Boolean, age: Int)

SchemaBuilder[Foo]