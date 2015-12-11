import java.io.FileOutputStream

import com.sksamuel.avro4s.{RecordSchemaFields, AvroSchema2, RecordSchemaBuilder}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import shapeless._
import shapeless.labelled._
import scala.reflect.ClassTag

trait Writer[A] {
  def apply(name: String, value: A, record: GenericRecord): Unit = record.put(name, value.toString)
}

object Writer {

  implicit object StringParser extends Writer[String]

  implicit object LongParser extends Writer[Long]

  implicit object IntParser extends Writer[Int]

  implicit object BooleanParser extends Writer[Boolean]

  implicit object HNilParser extends Writer[HNil] {
    override def apply(name: String, value: HNil, record: GenericRecord): Unit = ()
  }
}

trait FieldWrites[L <: HList] extends DepFn0 with Serializable {
  type Out = Unit
}

object FieldWrites {

  implicit object HNilFields extends RecordSchemaFields[HNil] {
    def apply() = List.empty
  }

  implicit def HConsFields[K <: Symbol, V, T <: HList](implicit key: Witness.Aux[K],
                                                       writer: Writer[V],
                                                       remaining: RecordSchemaFields[T],
                                                       tag: ClassTag[V]): FieldWrites[FieldType[K, V] :: T] = {
    new FieldWrites[FieldType[K, V] :: T] {
      def apply: Unit = {
        writer(key.value.name, value, record)
        Nil
      }
    }
  }
}

trait AvroSer[T] {
  def toRecord(t: T): GenericRecord
}

object AvroSer {

  implicit def GenericSer[T, Repr <: HList](implicit labl: LabelledGeneric.Aux[T, Repr],
                                            schema: RecordSchemaBuilder[T]) = new AvroSer[T] {
    override def toRecord(t: T): GenericRecord = {
      val r = new org.apache.avro.generic.GenericData.Record(schema())
      writer(t)
      r
    }
  }
  def apply[T](t: T)(implicit ser: AvroSer[T]): GenericRecord = ser.toRecord(t)
}

case class Fibble(boo: String)

val fibble = Fibble("sam")
val out = new FileOutputStream("test.avro")
val datumWriter = new GenericDatumWriter[GenericRecord](AvroSchema2[Fibble])
val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
dataFileWriter.create(AvroSchema2[Fibble], out)
val r = AvroSer(fibble)
dataFileWriter.append(r)
dataFileWriter.flush()
dataFileWriter.close()