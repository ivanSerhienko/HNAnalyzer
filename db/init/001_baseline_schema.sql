-- Baseline schema: bronze / silver / gold, per
-- docs/superpowers/specs/2026-08-20-hn-medallion-analysis-design.md and
-- docs/superpowers/specs/2026-08-20-hn-transform-mechanics-design.md

-- === Bronze ===================================================

CREATE TABLE bronze (
    id              BIGSERIAL PRIMARY KEY,
    entity_type     TEXT        NOT NULL,
    entity_id       BIGINT      NOT NULL,
    source_listing  TEXT,
    poll_id         UUID        NOT NULL,
    polled_at       TIMESTAMPTZ NOT NULL,
    raw_json        JSONB       NOT NULL
);

CREATE INDEX idx_bronze_poll_id ON bronze (poll_id);
CREATE INDEX idx_bronze_entity ON bronze (entity_type, entity_id);

-- === Silver ====================================================

CREATE TABLE story_snapshots (
    story_id        BIGINT      NOT NULL,
    poll_id         UUID        NOT NULL,
    polled_at       TIMESTAMPTZ NOT NULL,
    title           TEXT        NOT NULL,
    url             TEXT,
    domain          TEXT,
    author          TEXT,
    score           INT         NOT NULL,
    comment_count   INT         NOT NULL,
    story_type      TEXT        NOT NULL,
    submitted_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (story_id, poll_id)
);

CREATE INDEX idx_story_snapshots_polled_at ON story_snapshots (polled_at);
CREATE INDEX idx_story_snapshots_domain ON story_snapshots (domain);

CREATE TABLE comments (
    comment_id      BIGINT      PRIMARY KEY,
    story_id        BIGINT      NOT NULL,
    author          TEXT,
    text            TEXT,
    submitted_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_comments_story_id ON comments (story_id);

-- Reference table for trending_keywords tokenization filtering.
CREATE TABLE stopwords (
    word            TEXT        PRIMARY KEY
);

-- === Gold ======================================================

CREATE TABLE trending_keywords (
    keyword         TEXT        NOT NULL,
    poll_id         UUID        NOT NULL,
    recent_count    INT         NOT NULL,
    baseline_count  INT         NOT NULL,
    spike_score     NUMERIC     NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (keyword, poll_id)
);

CREATE INDEX idx_trending_keywords_computed_at ON trending_keywords (computed_at);

CREATE TABLE story_velocity (
    story_id            BIGINT      NOT NULL,
    poll_id             UUID        NOT NULL,
    points_per_hour     NUMERIC     NOT NULL,
    comments_per_hour   NUMERIC     NOT NULL,
    computed_at         TIMESTAMPTZ NOT NULL,
    UNIQUE (story_id, poll_id)
);

CREATE INDEX idx_story_velocity_computed_at ON story_velocity (computed_at);

CREATE TABLE domain_stats (
    domain          TEXT        NOT NULL,
    poll_id         UUID        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    story_count     INT         NOT NULL,
    avg_score       NUMERIC     NOT NULL,
    trend_direction TEXT        NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (domain, poll_id)
);

CREATE INDEX idx_domain_stats_computed_at ON domain_stats (computed_at);
