
//class RecordFormatTest extends WordSpec with Matchers {
//  case class Composer(name: String, birthplace: String, compositions: Seq[String])
//
//  case class Book(title: String)
//  case class Song(title: String, lyrics: String)
//  case class Author[T](name: String, birthplace: String, work: Seq[T])
//
//  "RecordFormat" should {

//    "convert to/from records of generic classes" in {
//      val author1 = Author[Book]("Heraclitus", "Ephesus", Seq(Book("Panta Rhei")))
//      val fmt = RecordFormat[Author[Book]]
//      fmt.from(fmt.to(author1)) shouldBe author1
//    }
//  }
//}
