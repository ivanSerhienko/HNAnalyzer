package ingest

import zio._
import zio.http._
import zio.test._
import zio.test.Assertion._

object HackerNewsClientSpec extends ZIOSpecDefault {
  def spec = suite("HackerNewsClient")(
    test("handleResponse fails on a non-2xx response") {
      val response = Response(status = Status.TooManyRequests)
      assertZIO(HackerNewsClient.handleResponse(response).exit)(
        fails(hasMessage(containsString("429"))),
      )
    },
  )
}
