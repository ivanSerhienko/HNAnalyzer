import zio._
import zio.http.Client
import zio.json._

import ingest.{HackerNewsClient, Item, Listing}
import storage.{BronzeRow, BronzeWriter, Db, GoldComputer, SilverTransformer}

import java.time.Instant
import java.util.UUID

object App extends ZIOAppDefault {

  private val listings          = List(Listing.Top, Listing.New, Listing.Ask, Listing.Show)
  private val storiesPerListing = 3
  private val goldTopN          = 5

  private val pollInterval = 30.minutes

  private def writeBronze(
    entityType: String,
    entityId: Long,
    sourceListing: Option[Listing],
    pollId: UUID,
    polledAt: Instant,
    rawJson: String,
  ) =
    BronzeWriter
      .write(BronzeRow(entityType, entityId, sourceListing.map(_.toString.toLowerCase), pollId, polledAt, rawJson))
      .tapError(e => ZIO.logError(s"failed to write $entityType $entityId to bronze: ${e.getMessage}"))

  private def processComment(commentId: Long, storyId: Long, pollId: UUID, polledAt: Instant) =
    for {
      raw <- HackerNewsClient
               .fetchItem(commentId)
               .tapError(e => ZIO.logError(s"failed to fetch comment $commentId (story $storyId): ${e.getMessage}"))
      _   <- writeBronze("comment", commentId, None, pollId, polledAt, raw)
      _   <- ZIO.logInfo(s"wrote comment $commentId (story $storyId)")
    } yield ()

  private def processStory(storyId: Long, listing: Listing, pollId: UUID, polledAt: Instant) =
    for {
      raw  <- HackerNewsClient
                .fetchItem(storyId)
                .tapError(e => ZIO.logError(s"[$listing] failed to fetch story $storyId: ${e.getMessage}"))
      _    <- writeBronze("story", storyId, Some(listing), pollId, polledAt, raw)
      item <- ZIO
                .fromEither(raw.fromJson[Item])
                .mapError(e => new RuntimeException(s"failed to decode story $storyId: $e"))
                .tapError(e => ZIO.logError(e.getMessage))
      kids  = item.kids.getOrElse(Nil)
      _    <- ZIO.logInfo(
                s"[$listing] wrote story $storyId \"${item.title.getOrElse("-")}\" (${kids.size} comments)",
              )
      _    <- ZIO.foreachDiscard(kids)(processComment(_, storyId, pollId, polledAt))
    } yield ()

  private def processListing(listing: Listing, pollId: UUID, polledAt: Instant) =
    for {
      ids <- HackerNewsClient
               .fetchListing(listing)
               .tapError(e => ZIO.logError(s"[$listing] failed to fetch listing: ${e.getMessage}"))
      _   <- ZIO.logInfo(s"[$listing] fetched ${ids.size} ids, processing top $storiesPerListing")
      _   <- ZIO.foreachDiscard(ids.take(storiesPerListing))(processStory(_, listing, pollId, polledAt))
    } yield ()

  private def runSilver(pollId: UUID) =
    for {
      stories  <- SilverTransformer
                    .transformStories(pollId)
                    .tapError(e => ZIO.logError(s"silver: failed to transform stories: ${e.getMessage}"))
      comments <- SilverTransformer
                    .transformComments(pollId)
                    .tapError(e => ZIO.logError(s"silver: failed to transform comments: ${e.getMessage}"))
      _        <- ZIO.logInfo(s"silver complete: $stories story_snapshots, $comments comments inserted")
    } yield ()

  private def runGold(pollId: UUID) =
    for {
      keywordCount <- GoldComputer
                         .computeTrendingKeywords(pollId)
                         .tapError(e => ZIO.logError(s"gold: failed computing trending keywords: ${e.getMessage}"))
      velocityCount <- GoldComputer
                          .computeStoryVelocity(pollId)
                          .tapError(e => ZIO.logError(s"gold: failed computing story velocity: ${e.getMessage}"))
      domainCount   <- GoldComputer
                          .computeDomainStats(pollId)
                          .tapError(e => ZIO.logError(s"gold: failed computing domain stats: ${e.getMessage}"))
      topKeywords   <- GoldComputer.topTrendingKeywords(pollId, goldTopN)
      topVelocity   <- GoldComputer.topVelocity(pollId, goldTopN)
      topDomains    <- GoldComputer.topRisingDomains(pollId, goldTopN)
      _             <- ZIO.logInfo(
                          s"gold complete: $keywordCount keywords (top: ${topKeywords.mkString(", ")}), " +
                            s"$velocityCount velocity rows (top: ${topVelocity.mkString(", ")}), " +
                            s"$domainCount domain stats (top: ${topDomains.mkString(", ")})",
                        )
    } yield ()

  private val pollCycle =
    for {
      pollId   <- ZIO.succeed(UUID.randomUUID())
      polledAt <- Clock.instant
      _        <- ZIO.logInfo(s"poll $pollId starting at $polledAt")
      _        <- ZIO.foreachDiscard(listings)(processListing(_, pollId, polledAt))
      _        <- runSilver(pollId)
      _        <- runGold(pollId)
      _        <- ZIO.logInfo(s"poll $pollId complete")
    } yield ()

  def run =
    pollCycle
      .catchAllCause(cause => ZIO.logErrorCause("poll cycle failed", cause))
      .repeat(Schedule.spaced(pollInterval))
      .provide(
        HackerNewsClient.live,
        Client.default,
        Db.pool,
        BronzeWriter.live,
        SilverTransformer.live,
        GoldComputer.live,
      )
}
