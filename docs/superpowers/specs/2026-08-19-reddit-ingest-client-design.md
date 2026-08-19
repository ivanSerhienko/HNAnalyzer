# Reddit Ingest Client (first slice) — Design

## Goal

First concrete step of the ingest stage of the ETL pipeline described in
CLAUDE.md: fetch `https://www.reddit.com/best.json` and print the raw
response body from `Main`. No parsing, no streaming/polling loop, no
persistence yet — those are separate, later slices.

## Dependency

Add `zio-http` (client) to `build.sbt`, version-aligned with the existing
`zioVersion` where the artifact tracks it, otherwise its own compatible
release for ZIO `2.1.19`.

## Architecture / components

New package `ingest` under `src/main/scala`:

- `ingest/RedditClient.scala`
  - `trait RedditClient { def fetchBest: Task[String] }`
  - `RedditClient.Live` — implementation built on zio-http's `Client`,
    issues `GET https://www.reddit.com/best.json`, returns the response
    body as a `String`.
  - `RedditClient.live: ZLayer[Client, Nothing, RedditClient]`
  - `RedditClient.fetchBest: ZIO[RedditClient, Throwable, String]` —
    standard ZIO accessor.

`Main.scala` (object `App extends ZIOAppDefault`) wires:

```scala
def run =
  RedditClient.fetchBest
    .flatMap(body => Console.printLine(body))
    .provide(RedditClient.live, Client.default)
```

## Data flow

`Main` → `RedditClient.fetchBest` → zio-http `Client` issues
`GET https://www.reddit.com/best.json` → response body read as `String`
→ printed as-is.

## Reddit-specific requirement: User-Agent

Reddit's public JSON endpoints reject requests carrying a generic/default
`User-Agent` (commonly returning 429 or a blocked-content page instead of
JSON), even without OAuth. The client request must set an explicit,
descriptive `User-Agent` header, e.g. `RedditAnalyzer/0.1`.

For this slice the value is **hardcoded** in `RedditClient.Live` (not
env-configurable). Revisit if/when it needs to vary per environment.

## Error handling

No retries or backoff in this slice — that belongs to the later
polling/streaming ingest loop. Network errors or non-2xx responses
surface as a `Throwable` failure on `fetchBest`; `ZIOAppDefault` prints
the stack trace and exits non-zero. Sufficient for a "does this work"
step.

## Testing

No automated test for this slice. The endpoint is live and external;
there is no meaningful unit-test target without a fake `Client`, and
CLAUDE.md's testing conventions avoid mocked integration tests.
Verification is manual: run `sbt run` and confirm JSON prints to stdout.
Automated tests return once there is parsing/analysis logic to check
against fixtures.

## Explicitly out of scope for this slice

- JSON parsing / decoding into domain types
- Continuous polling / zio-streams loop
- Retry/backoff policy
- Env-configurable User-Agent or other ingest config
- Persistence (transform/report stages)
