package ingest

import zio._
import zio.http._

trait RedditClient {
  def fetchBest: Task[String]
}

object RedditClient {

  private val bestUrl = "https://www.reddit.com/best.json"

  private val userAgentHeader: Header.UserAgent =
    Header.UserAgent(
      Header.UserAgent.ProductOrComment.Product("RedditAnalyzer", Some("0.1")),
    )

  def handleResponse(response: Response): Task[String] =
    if (response.status.isSuccess)
      response.body.asString
    else
      ZIO.fail(
        new RuntimeException(
          s"Reddit returned ${response.status.code} ${response.status}: request failed",
        ),
      )

  final case class Live(client: Client) extends RedditClient {
    override def fetchBest: Task[String] =
      (for {
        response <- client.batched(Request.get(bestUrl).addHeader(userAgentHeader))
        body     <- handleResponse(response)
      } yield body)
        .timeout(30.seconds)
        .someOrFail(new RuntimeException("Request to Reddit timed out after 30s"))
  }

  val live: ZLayer[Client, Nothing, RedditClient] =
    ZLayer.fromFunction(Live.apply _)

  def fetchBest: ZIO[RedditClient, Throwable, String] =
    ZIO.serviceWithZIO[RedditClient](_.fetchBest)
}
