package benchmarks

import java.time.Instant

trait BenchmarkHelpers {

  val now: Instant = Instant.now
  var counter = 0L

  def t: Instant = {
    counter += 1
    now.minusSeconds(counter)
  }

}
