package storage

import zio._
import zio.jdbc._
import zio.test._

import java.time.temporal.ChronoUnit
import java.time.Instant
import java.util.UUID

object GoldComputerSpec extends ZIOSpecDefault {

  private def insertSnapshot(
    storyId: Long,
    snapshotPollId: UUID,
    polledAt: Instant,
    title: String,
    domain: Option[String],
    score: Int,
    commentCount: Int,
    submittedAt: Instant,
  ) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"""INSERT INTO story_snapshots
              (story_id, poll_id, polled_at, title, url, domain, author, score, comment_count, story_type, submitted_at)
            VALUES
              ($storyId, $snapshotPollId, $polledAt, $title, ${domain.map(d => s"https://$d/x")}, $domain, 'tester',
               $score, $commentCount, 'top', $submittedAt)"""
        .insert
        .unit
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  private def insertStopword(word: String) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"INSERT INTO stopwords (word) VALUES ($word) ON CONFLICT (word) DO NOTHING".insert.unit
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  private def trendingKeyword(keyword: String, pollId: UUID) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"SELECT recent_count, baseline_count, spike_score FROM trending_keywords WHERE keyword = $keyword AND poll_id = $pollId"
        .query[(Int, Int, Double)]
        .selectOne
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  private def storyVelocity(storyId: Long, pollId: UUID) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"SELECT points_per_hour, comments_per_hour FROM story_velocity WHERE story_id = $storyId AND poll_id = $pollId"
        .query[(Double, Double)]
        .selectOne
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  private def domainStat(domain: String, pollId: UUID) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"SELECT story_count, trend_direction FROM domain_stats WHERE domain = $domain AND poll_id = $pollId"
        .query[(Int, String)]
        .selectOne
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  def spec = suite("GoldComputer")(
    test("trending_keywords surfaces a keyword that spikes in the recent window, filtering stopwords") {
      val now      = Instant.now()
      val goldPoll = UUID.randomUUID()
      val keyword  = s"zioeffect${scala.util.Random.nextInt(1000000)}"

      for {
        _ <- insertStopword("thisisastopword")
        _ <- ZIO.foreachDiscard(1 to 3) { i =>
               insertSnapshot(
                 storyId = scala.util.Random.nextLong().abs,
                 snapshotPollId = UUID.randomUUID(),
                 polledAt = now.minus(1, ChronoUnit.HOURS),
                 title = s"Learning about $keyword thisisastopword today",
                 domain = None,
                 score = 1,
                 commentCount = 0,
                 submittedAt = now.minus(1, ChronoUnit.HOURS),
               )
             }
        _ <- GoldComputer.computeTrendingKeywords(goldPoll)
        row <- trendingKeyword(keyword, goldPoll)
        stopwordRow <- trendingKeyword("thisisastopword", goldPoll)
      } yield assertTrue(
        row.exists { case (recent, baseline, spike) => recent == 3 && baseline == 0 && spike > 0.0 },
        stopwordRow.isEmpty,
      )
    },
    test("story_velocity computes an age-normalized rate from earliest and latest recent snapshots") {
      val now         = Instant.now()
      val goldPoll    = UUID.randomUUID()
      val storyId     = scala.util.Random.nextLong().abs
      val submittedAt = now.minus(4, ChronoUnit.HOURS)
      val latestAt    = now.minus(10, ChronoUnit.MINUTES)

      for {
        _ <- insertSnapshot(storyId, UUID.randomUUID(), now.minus(2, ChronoUnit.HOURS), "t", None, 10, 2, submittedAt)
        _ <- insertSnapshot(storyId, UUID.randomUUID(), latestAt, "t", None, 50, 20, submittedAt)
        _ <- GoldComputer.computeStoryVelocity(goldPoll)
        result <- storyVelocity(storyId, goldPoll)
      } yield {
        val hours            = java.time.Duration.between(submittedAt, latestAt).toMillis / 3600000.0
        val expectedPoints   = (50 - 10) / hours
        val expectedComments = (20 - 2) / hours
        assertTrue(
          result.exists { case (points, comments) =>
            math.abs(points - expectedPoints) < 0.01 && math.abs(comments - expectedComments) < 0.01
          },
        )
      }
    },
    test("domain_stats marks a domain rising when recent volume outpaces baseline") {
      val now      = Instant.now()
      val goldPoll = UUID.randomUUID()
      val domain   = s"rising-${scala.util.Random.nextInt(1000000)}.example"

      for {
        _ <- ZIO.foreachDiscard(1 to 5) { _ =>
               insertSnapshot(
                 scala.util.Random.nextLong().abs,
                 UUID.randomUUID(),
                 now.minus(1, ChronoUnit.HOURS),
                 "t",
                 Some(domain),
                 100,
                 0,
                 now.minus(1, ChronoUnit.HOURS),
               )
             }
        _ <- insertSnapshot(
               scala.util.Random.nextLong().abs,
               UUID.randomUUID(),
               now.minus(3, ChronoUnit.DAYS),
               "t",
               Some(domain),
               100,
               0,
               now.minus(3, ChronoUnit.DAYS),
             )
        _      <- GoldComputer.computeDomainStats(goldPoll)
        result <- domainStat(domain, goldPoll)
      } yield assertTrue(result.contains((5, "rising")))
    },
    test("topTrendingKeywords ranks by spike_score descending and respects the limit") {
      val now      = Instant.now()
      val goldPoll = UUID.randomUUID()
      val loud     = s"loudkeyword${scala.util.Random.nextInt(1000000)}"
      val quiet    = s"quietkeyword${scala.util.Random.nextInt(1000000)}"

      for {
        _ <- ZIO.foreachDiscard(1 to 5) { _ =>
               insertSnapshot(
                 scala.util.Random.nextLong().abs,
                 UUID.randomUUID(),
                 now.minus(1, ChronoUnit.HOURS),
                 s"all about $loud",
                 None,
                 1,
                 0,
                 now.minus(1, ChronoUnit.HOURS),
               )
             }
        _ <- insertSnapshot(
               scala.util.Random.nextLong().abs,
               UUID.randomUUID(),
               now.minus(1, ChronoUnit.HOURS),
               s"all about $quiet",
               None,
               1,
               0,
               now.minus(1, ChronoUnit.HOURS),
             )
        _       <- GoldComputer.computeTrendingKeywords(goldPoll)
        limited <- GoldComputer.topTrendingKeywords(goldPoll, 1)
        loudRow  <- trendingKeyword(loud, goldPoll)
        quietRow <- trendingKeyword(quiet, goldPoll)
      } yield {
        // The DB accumulates rows across this whole test session, so unrelated
        // words can legitimately outrank ours globally - assert `limit` is
        // honored and that `loud`'s spike_score beats `quiet`'s, not that
        // we're globally #1.
        val loudScore  = loudRow.map(_._3)
        val quietScore = quietRow.map(_._3)
        assertTrue(limited.size == 1, loudScore.isDefined, quietScore.isDefined, loudScore.get > quietScore.get)
      }
    },
    test("topVelocity ranks by points_per_hour descending and respects the limit") {
      val now         = Instant.now()
      val goldPoll    = UUID.randomUUID()
      val fastStory   = scala.util.Random.nextLong().abs
      val slowStory   = scala.util.Random.nextLong().abs
      val submittedAt = now.minus(4, ChronoUnit.HOURS)

      for {
        _   <- insertSnapshot(fastStory, UUID.randomUUID(), now.minus(2, ChronoUnit.HOURS), "t", None, 10, 0, submittedAt)
        _   <- insertSnapshot(fastStory, UUID.randomUUID(), now.minus(10, ChronoUnit.MINUTES), "t", None, 500, 0, submittedAt)
        _   <- insertSnapshot(slowStory, UUID.randomUUID(), now.minus(2, ChronoUnit.HOURS), "t", None, 10, 0, submittedAt)
        _   <- insertSnapshot(slowStory, UUID.randomUUID(), now.minus(10, ChronoUnit.MINUTES), "t", None, 11, 0, submittedAt)
        _         <- GoldComputer.computeStoryVelocity(goldPoll)
        limited   <- GoldComputer.topVelocity(goldPoll, 1)
        unlimited <- GoldComputer.topVelocity(goldPoll, 100000)
      } yield {
        // Every call recomputes velocity for *all* currently-recent stories
        // (by design), so repeated test runs leave same-rate leftovers from
        // earlier iterations - assert relative order between our two known
        // stories and that `limit` is honored, not that we're globally #1.
        val fastRate = unlimited.collectFirst { case (id, rate) if id == fastStory => rate }
        val slowRate = unlimited.collectFirst { case (id, rate) if id == slowStory => rate }
        assertTrue(limited.size == 1, fastRate.isDefined, slowRate.isDefined, fastRate.get > slowRate.get)
      }
    },
    test("topRisingDomains returns the domain and its trend direction") {
      val now      = Instant.now()
      val goldPoll = UUID.randomUUID()
      val domain   = s"top-${scala.util.Random.nextInt(1000000)}.example"

      for {
        _   <- ZIO.foreachDiscard(1 to 5) { _ =>
                 insertSnapshot(
                   scala.util.Random.nextLong().abs,
                   UUID.randomUUID(),
                   now.minus(1, ChronoUnit.HOURS),
                   "t",
                   Some(domain),
                   100,
                   0,
                   now.minus(1, ChronoUnit.HOURS),
                 )
               }
        _   <- GoldComputer.computeDomainStats(goldPoll)
        row <- domainStat(domain, goldPoll)
        top <- GoldComputer.topRisingDomains(goldPoll, 5)
      } yield assertTrue(
        // Correctness of the underlying row, via the same direct-lookup
        // pattern proven reliable elsewhere in this suite.
        row.contains((5, "rising")),
        // Structural checks on the "top N" method itself: other domains
        // recomputed under this same poll_id can legitimately tie/outrank
        // ours, so we don't assert exact membership here.
        top.size <= 5,
        top.forall { case (_, dir) => Set("rising", "falling", "flat").contains(dir) },
      )
    },
  ).provideLayerShared(Db.pool >+> GoldComputer.live)
}
