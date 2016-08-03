package com.sksamuel.avro4s

import org.scalatest.{Matchers, WordSpec}
import java.nio.file.Paths

class FileRendererTest extends WordSpec with Matchers {
  "FileRenderer" should {
    "write files with the right name" in {
      FileRenderer.output(Paths.get("/dir"), Seq(Template("com.some.package.Klass", "scala", "contents"))) shouldBe Map(Paths.get("/dir/com/some/package/Klass.scala") -> "contents")
    }
  }
}
