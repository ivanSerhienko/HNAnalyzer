package ingest

import zio._
import zio.http._

trait HackerNewsClient {
  def fetchBest: Task[String]
}

object HackerNewsClient {

  private val bestUrl = "https://hacker-news.firebaseio.com/v0/topstories.json"
  private def itemUrl(id: String) = s"https://hacker-news.firebaseio.com/v0/item/$id.json"

  private val userAgentHeader: Header.UserAgent =
    Header.UserAgent(
      Header.UserAgent.ProductOrComment.Product("HNAnalyzer", Some("0.1")),
    )

  def handleResponse(response: Response): Task[String] =
    if (response.status.isSuccess)
      response.body.asString
    else
      ZIO.fail(
        new RuntimeException(
          s"Hacker News returned ${response.status.code} ${response.status}: request failed",
        ),
      )

  // POC-only crude parsing: pull the first element out of a `"key":[...]` array
  // in a raw JSON string. No JSON lib yet - just proving the shape of the data.
  private def firstArrayElement(json: String, key: String): Option[String] = {
    val marker = s"\"$key\":["
    val start  = json.indexOf(marker)
    if (start < 0) None
    else {
      val afterMarker = json.substring(start + marker.length)
      val end         = afterMarker.indexOf(']')
      afterMarker.substring(0, end).split(",").headOption.map(_.trim)
    }
  }

  final case class Live(client: Client) extends HackerNewsClient {
    private def get(url: String): Task[String] =
      client.batched(Request.get(url).addHeader(userAgentHeader)).flatMap(handleResponse)

    override def fetchBest: Task[String] =
      (for {
        listBody    <- get(bestUrl)
        firstId      = listBody.stripPrefix("[").stripSuffix("]").split(",").head.trim
        storyBody   <- get(itemUrl(firstId))
        commentBody <- firstArrayElement(storyBody, "kids") match {
                         case Some(commentId) => get(itemUrl(commentId))
                         case None            => ZIO.succeed("(story has no comments)")
                       }
      } yield s"STORY:\n$storyBody\n\nFIRST COMMENT:\n$commentBody")
        .timeout(30.seconds)
        .someOrFail(new RuntimeException("Request to Hacker News timed out after 30s"))
  }

  val live: ZLayer[Client, Nothing, HackerNewsClient] =
    ZLayer.fromFunction(Live.apply _)

  def fetchBest: ZIO[HackerNewsClient, Throwable, String] =
    ZIO.serviceWithZIO[HackerNewsClient](_.fetchBest)
}
