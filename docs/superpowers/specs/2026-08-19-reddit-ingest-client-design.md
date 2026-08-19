# Reddit Ingest Client (first slice) — Design

## Goal

First concrete step of the ingest stage of the ETL pipeline described in
CLAUDE.md: fetch `https://www.reddit.com/best.json` and print the raw
response body from `Main`. No parsing, no streaming/polling loop, no
persistence yet — those are separate, later slices.

## Dependency

Add `zio-http` `3.11.4` to `build.sbt` (latest release as of 2026-08-18).
It transitively depends on `zio` `2.1.26`, so `zioVersion` in `build.sbt`
is bumped from `2.1.19` to `2.1.26` to stay explicitly aligned rather than
rely on sbt's implicit eviction to the higher version.

## Architecture / components

New package `ingest` under `src/main/scala`:

- `ingest/RedditClient.scala`
  - `trait RedditClient { def fetchBest: Task[String] }`
  - `RedditClient.handleResponse(response: Response): Task[String]` — pure
    companion-object function: fails on a non-2xx status, otherwise reads
    the body as a `String`. Pulled out as its own function specifically so
    it's unit-testable with a canned `Response` value, with no need to
    fake zio-http's `Client` trait.
  - `RedditClient.Live` — implementation built on zio-http's `Client`.
    `fetchBest` issues `GET https://www.reddit.com/best.json`, pipes the
    result through `handleResponse`, and wraps the whole sequence
    (request + status check + body read) in a 30-second timeout.
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
polling/streaming ingest loop. Three failure modes are handled explicitly:

- **Non-2xx status**: `handleResponse` checks `response.status.isSuccess`
  and fails with `RuntimeException(s"Reddit returned ${response.status}: request failed")`
  before reading the body, instead of silently returning error-page content
  as if it were a normal response.
- **Timeout**: the full fetch-and-validate sequence in `Live.fetchBest` is
  wrapped in `.timeout(30.seconds).someOrFail(...)`, failing with
  `RuntimeException("Request to Reddit timed out after 30s")` rather than
  hanging indefinitely.
- **Everything else** (network errors, unexpected exceptions): surfaces as
  a `Throwable` failure on `fetchBest`; `ZIOAppDefault` prints the stack
  trace and exits non-zero.

Accepted residual risk: a 200 OK response carrying an HTML "blocked
content" page instead of JSON would pass the status check and get printed
as garbage. Catching that would require inspecting response *content*,
which is out of scope here (that's parsing). Manual eyeballing of `sbt
run` output is the accepted mitigation for this slice.

zio-http's `Client.default` does not auto-follow redirects (that's opt-in
via `ZClientAspect.followRedirects`). This isn't handled here because it
isn't needed: the client targets the canonical `https://www.reddit.com/best.json`
directly, so there's no redirect chain to traverse for this specific URL.

## Testing

One minimal `zio-test` case, in `src/test/scala/ingest/RedditClientSpec.scala`:
feed `RedditClient.handleResponse` a canned non-2xx `Response` and assert
it fails with the expected message. No fake `Client` needed, since
`handleResponse` is a pure function over a `Response` value. The timeout
branch is not tested — faking a hang meaningfully isn't worth it here.

Everything else about fetching remains manually verified: run `sbt run`
and confirm JSON prints to stdout. Broader automated coverage (e.g. of
`Live.fetchBest` itself) returns once there is parsing/analysis logic to
check against fixtures.

## Explicitly out of scope for this slice

- JSON parsing / decoding into domain types
- Continuous polling / zio-streams loop
- Retry/backoff policy
- Env-configurable User-Agent or other ingest config
- Persistence (transform/report stages)
