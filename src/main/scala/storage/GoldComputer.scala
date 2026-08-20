package storage

import zio._
import zio.jdbc._

import java.util.UUID

trait GoldComputer {
  def computeTrendingKeywords(pollId: UUID): Task[Long]
  def computeStoryVelocity(pollId: UUID): Task[Long]
  def computeDomainStats(pollId: UUID): Task[Long]

  // Read-only lookups over this cycle's gold rows, for reporting/logging.
  def topTrendingKeywords(pollId: UUID, limit: Int): Task[List[(String, Double)]]
  def topVelocity(pollId: UUID, limit: Int): Task[List[(Long, Double)]]
  def topRisingDomains(pollId: UUID, limit: Int): Task[List[(String, String)]]
}

object GoldComputer {

  final case class Live(pool: ZConnectionPool) extends GoldComputer {
    private def run(fragment: SqlFragment): Task[Long] =
      fragment.insert.provideLayer(ZLayer.succeed(pool) >>> transaction)

    private def runQuery[A: JdbcDecoder](fragment: SqlFragment): Task[List[A]] =
      fragment.query[A].selectAll.map(_.toList).provideLayer(ZLayer.succeed(pool) >>> transaction)

    override def topTrendingKeywords(pollId: UUID, limit: Int): Task[List[(String, Double)]] =
      runQuery(
        sql"SELECT keyword, spike_score FROM trending_keywords WHERE poll_id = $pollId ORDER BY spike_score DESC LIMIT $limit",
      )

    override def topVelocity(pollId: UUID, limit: Int): Task[List[(Long, Double)]] =
      runQuery(
        sql"SELECT story_id, points_per_hour FROM story_velocity WHERE poll_id = $pollId ORDER BY points_per_hour DESC LIMIT $limit",
      )

    override def topRisingDomains(pollId: UUID, limit: Int): Task[List[(String, String)]] =
      runQuery(
        sql"SELECT domain, trend_direction FROM domain_stats WHERE poll_id = $pollId ORDER BY story_count DESC LIMIT $limit",
      )

    // Titles/comments are tokenized once per story/comment (not once per poll
    // snapshot) using each story's fixed submitted_at for windowing, since
    // story_snapshots is append-only and would otherwise over-count stories
    // that have simply stayed listed across many poll cycles.
    override def computeTrendingKeywords(pollId: UUID): Task[Long] =
      run(sql"""
        WITH story_words AS (
          SELECT DISTINCT ON (story_id) story_id, title, submitted_at
          FROM story_snapshots
          WHERE polled_at >= now() - interval '7 days'
          ORDER BY story_id, polled_at DESC
        ),
        tokens AS (
          SELECT lower(word) AS keyword, submitted_at AS ts FROM story_words, regexp_split_to_table(title, '\s+') AS word
          UNION ALL
          SELECT lower(word) AS keyword, submitted_at AS ts FROM comments, regexp_split_to_table(coalesce(text, ''), '\s+') AS word
          WHERE submitted_at >= now() - interval '7 days'
        ),
        cleaned AS (
          SELECT regexp_replace(keyword, '[^a-z0-9]', '', 'g') AS keyword, ts
          FROM tokens
        ),
        filtered AS (
          SELECT keyword, ts
          FROM cleaned
          WHERE length(keyword) > 2 AND keyword NOT IN (SELECT word FROM stopwords)
        ),
        counts AS (
          SELECT
            keyword,
            count(*) FILTER (WHERE ts >= now() - interval '24 hours')  AS recent_count,
            count(*) FILTER (WHERE ts <  now() - interval '24 hours')  AS baseline_count
          FROM filtered
          GROUP BY keyword
        )
        INSERT INTO trending_keywords (keyword, poll_id, recent_count, baseline_count, spike_score, computed_at)
        SELECT
          keyword,
          $pollId,
          recent_count,
          baseline_count,
          (recent_count / 24.0) - (baseline_count / 168.0),
          now()
        FROM counts
        WHERE recent_count > 0
        ON CONFLICT (keyword, poll_id) DO NOTHING
      """)

    override def computeStoryVelocity(pollId: UUID): Task[Long] =
      run(sql"""
        WITH windowed AS (
          SELECT story_id, score, comment_count, polled_at, submitted_at
          FROM story_snapshots
          WHERE polled_at >= now() - interval '24 hours'
        ),
        agg AS (
          SELECT
            story_id,
            min(submitted_at) AS submitted_at,
            (array_agg(score ORDER BY polled_at ASC))[1]          AS first_score,
            (array_agg(score ORDER BY polled_at DESC))[1]         AS last_score,
            (array_agg(comment_count ORDER BY polled_at ASC))[1]  AS first_comments,
            (array_agg(comment_count ORDER BY polled_at DESC))[1] AS last_comments,
            max(polled_at) AS latest_polled_at,
            count(*) AS snapshot_count
          FROM windowed
          GROUP BY story_id
        )
        INSERT INTO story_velocity (story_id, poll_id, points_per_hour, comments_per_hour, computed_at)
        SELECT
          story_id,
          $pollId,
          (last_score - first_score) / GREATEST(extract(epoch FROM (latest_polled_at - submitted_at)) / 3600.0, 0.01),
          (last_comments - first_comments) / GREATEST(extract(epoch FROM (latest_polled_at - submitted_at)) / 3600.0, 0.01),
          now()
        FROM agg
        WHERE snapshot_count >= 2
        ON CONFLICT (story_id, poll_id) DO NOTHING
      """)

    override def computeDomainStats(pollId: UUID): Task[Long] =
      run(sql"""
        WITH story_domains AS (
          SELECT DISTINCT ON (story_id) story_id, domain, score, submitted_at
          FROM story_snapshots
          WHERE polled_at >= now() - interval '7 days' AND domain IS NOT NULL
          ORDER BY story_id, polled_at DESC
        ),
        per_domain AS (
          SELECT
            domain,
            count(*) FILTER (WHERE submitted_at >= now() - interval '24 hours')                       AS recent_count,
            avg(score) FILTER (WHERE submitted_at >= now() - interval '24 hours')                      AS recent_avg_score,
            count(*) FILTER (WHERE submitted_at < now() - interval '24 hours')                         AS baseline_count
          FROM story_domains
          GROUP BY domain
        )
        INSERT INTO domain_stats (domain, poll_id, window_start, window_end, story_count, avg_score, trend_direction, computed_at)
        SELECT
          domain,
          $pollId,
          now() - interval '24 hours',
          now(),
          recent_count,
          COALESCE(recent_avg_score, 0),
          CASE
            WHEN (recent_count / 24.0) > (baseline_count / 168.0) * 1.1 THEN 'rising'
            WHEN (recent_count / 24.0) < (baseline_count / 168.0) * 0.9 THEN 'falling'
            ELSE 'flat'
          END,
          now()
        FROM per_domain
        WHERE recent_count > 0
        ON CONFLICT (domain, poll_id) DO NOTHING
      """)
  }

  val live: ZLayer[ZConnectionPool, Nothing, GoldComputer] =
    ZLayer.fromFunction(Live.apply)

  def computeTrendingKeywords(pollId: UUID): ZIO[GoldComputer, Throwable, Long] =
    ZIO.serviceWithZIO[GoldComputer](_.computeTrendingKeywords(pollId))

  def computeStoryVelocity(pollId: UUID): ZIO[GoldComputer, Throwable, Long] =
    ZIO.serviceWithZIO[GoldComputer](_.computeStoryVelocity(pollId))

  def computeDomainStats(pollId: UUID): ZIO[GoldComputer, Throwable, Long] =
    ZIO.serviceWithZIO[GoldComputer](_.computeDomainStats(pollId))

  def topTrendingKeywords(pollId: UUID, limit: Int): ZIO[GoldComputer, Throwable, List[(String, Double)]] =
    ZIO.serviceWithZIO[GoldComputer](_.topTrendingKeywords(pollId, limit))

  def topVelocity(pollId: UUID, limit: Int): ZIO[GoldComputer, Throwable, List[(Long, Double)]] =
    ZIO.serviceWithZIO[GoldComputer](_.topVelocity(pollId, limit))

  def topRisingDomains(pollId: UUID, limit: Int): ZIO[GoldComputer, Throwable, List[(String, String)]] =
    ZIO.serviceWithZIO[GoldComputer](_.topRisingDomains(pollId, limit))
}
