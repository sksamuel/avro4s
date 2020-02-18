package benchmarks

import java.time.Instant

import org.scalameter.api.Gen

trait BenchmarkHelpers {
  val item: Gen[Int] = Gen.range("item")(1, 1, 1)

  val now: Instant = Instant.now
  var counter = 0L

  def t: Instant = {
    counter += 1
    now.minusSeconds(counter)
  }

}
