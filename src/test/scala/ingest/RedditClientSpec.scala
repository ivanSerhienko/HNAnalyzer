package ingest

import zio._
import zio.http._
import zio.test._
import zio.test.Assertion._

object RedditClientSpec extends ZIOSpecDefault {
  def spec = suite("RedditClient")(
    test("handleResponse fails on a non-2xx response") {
      val response = Response(status = Status.TooManyRequests)
      assertZIO(RedditClient.handleResponse(response).exit)(
        fails(hasMessage(containsString("429"))),
      )
    },
  )
}
