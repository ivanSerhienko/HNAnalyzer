package ingest

import zio._
import zio.http._
import zio.json._

// HN's item API is a single flat, polymorphic shape: which fields are
// present depends on `type` (story/comment/job/...) and on deletion state.
final case class Item(
  id: Long,
  `type`: Option[String] = None,
  by: Option[String] = None,
  time: Option[Long] = None,
  text: Option[String] = None,
  dead: Option[Boolean] = None,
  deleted: Option[Boolean] = None,
  parent: Option[Long] = None,
  kids: Option[List[Long]] = None,
  url: Option[String] = None,
  score: Option[Int] = None,
  title: Option[String] = None,
  descendants: Option[Int] = None,
) derives JsonDecoder

enum Listing derives CanEqual:
  case Top, New, Ask, Show

  def path: String = this match
    case Listing.Top  => "/topstories.json"
    case Listing.New  => "/newstories.json"
    case Listing.Ask  => "/askstories.json"
    case Listing.Show => "/showstories.json"

final case class HnConfig(baseUrl: String)

object HnConfig {
  val fromEnv: Task[HnConfig] =
    ZIO
      .attempt(Option(java.lang.System.getenv("HN_API_BASE_URL")))
      .someOrFail(
        new RuntimeException(
          "Missing required environment variable: HN_API_BASE_URL (copy .env.example to .env and fill it in)",
        ),
      )
      .map(HnConfig(_))
}

trait HackerNewsClient {
  def fetchListing(listing: Listing): Task[List[Long]]

  // Returns the raw JSON body
  // Decode locally via `raw.fromJson[Item]` where structured fields are needed.
  def fetchItem(id: Long): Task[String]
}

object HackerNewsClient {

  // HN rejects to provide data wihtout header
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

  def decodeJson[A](body: String)(using decoder: JsonDecoder[A]): Task[A] =
    ZIO.fromEither(decoder.decodeJson(body)).mapError { error =>
      new RuntimeException(s"Failed to decode Hacker News response: $error")
    }

  final case class Live(client: Client, baseUrl: String) extends HackerNewsClient {
    private def get(url: String): Task[String] =
      client.batched(Request.get(url).addHeader(userAgentHeader)).flatMap(handleResponse)

    override def fetchListing(listing: Listing): Task[List[Long]] =
      (get(baseUrl + listing.path).flatMap(decodeJson[List[Long]]))
        .timeout(30.seconds)
        .someOrFail(new RuntimeException("Request to Hacker News timed out after 30s"))

    override def fetchItem(id: Long): Task[String] =
      get(s"$baseUrl/item/$id.json")
        .timeout(30.seconds)
        .someOrFail(new RuntimeException("Request to Hacker News timed out after 30s"))
  }

  val live: ZLayer[Client, Throwable, HackerNewsClient] =
    ZLayer.fromZIO {
      for {
        config <- HnConfig.fromEnv
        client <- ZIO.service[Client]
      } yield Live(client, config.baseUrl)
    }

  def fetchListing(listing: Listing): ZIO[HackerNewsClient, Throwable, List[Long]] =
    ZIO.serviceWithZIO[HackerNewsClient](_.fetchListing(listing))

  def fetchItem(id: Long): ZIO[HackerNewsClient, Throwable, String] =
    ZIO.serviceWithZIO[HackerNewsClient](_.fetchItem(id))
}
