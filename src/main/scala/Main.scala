import zio._
import zio.http.Client

import ingest.RedditClient

object App extends ZIOAppDefault {
  def run =
    RedditClient.fetchBest
      .flatMap(Console.printLine(_))
      .provide(RedditClient.live, Client.default)
}