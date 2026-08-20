import zio._
import zio.http.Client

import ingest.HackerNewsClient

object App extends ZIOAppDefault {
  def run =
    HackerNewsClient.fetchBest
      .flatMap(Console.printLine(_))
      .provide(HackerNewsClient.live, Client.default)
}