# Reddit Ingest Client (first slice) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fetch `https://www.reddit.com/best.json` via a ZIO-native HTTP client and print the raw response body from `Main`.

**Architecture:** A new `ingest` package holds a `RedditClient` ZIO service (trait + `Live` implementation + `ZLayer`) built on zio-http's `Client`. `Main` resolves the service, fetches, and prints — no parsing, no polling loop yet.

**Tech Stack:** Scala 3.8.4, ZIO 2.1.26, zio-http 3.11.4, sbt 1.12.8.

**Spec:** `docs/superpowers/specs/2026-08-19-reddit-ingest-client-design.md`

## Global Constraints

- ZIO version across all `dev.zio` artifacts must be `2.1.26` (bumped from `2.1.19` because zio-http 3.11.4 transitively depends on zio 2.1.26 — see spec's dependency section and the approved chat decision to align explicitly rather than rely on sbt eviction).
- zio-http version is exactly `3.11.4` (Maven Central `dev.zio:zio-http_3:3.11.4`, latest release as of 2026-08-18).
- No JSON parsing, no zio-streams polling loop, no retries/backoff, no env-configurable User-Agent, no redirect-following in this slice (per spec's "Explicitly out of scope" and "Error handling").
- The `User-Agent` header value is hardcoded as `RedditAnalyzer/0.1` (not env-configurable) — Reddit's public JSON endpoints reject the zio-http default User-Agent (`Zio-Http-Client/<version> (Scala <version>)`), typically with 429 or a blocked-content response.
- A non-2xx response must fail explicitly (via `RedditClient.handleResponse`), and the whole fetch must be bounded by a 30-second timeout — both are the one piece of automated test coverage this slice has; see spec's "Error handling" and "Testing" sections.

---

### Task 1: Add zio-http dependency and align ZIO version

**Files:**
- Modify: `build.sbt` (all lines — full file rewrite below)
- Modify: `CLAUDE.md:19` (version string), `CLAUDE.md:21-25` (dependency list)

**Interfaces:**
- Produces: `zioVersion` val = `"2.1.26"`, `zio-http` dependency on the classpath at `3.11.4`, available to Task 2's `import zio.http._`.

- [ ] **Step 1: Update `build.sbt`**

Replace the full contents of `build.sbt` with:

```scala
ThisBuild / scalaVersion := "3.8.4"

val zioVersion = "2.1.26"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"          % zioVersion,
  "dev.zio" %% "zio-streams"  % zioVersion % Optional,
  "dev.zio" %% "zio-http"     % "3.11.4",
  "dev.zio" %% "zio-test"     % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test
)
```

- [ ] **Step 2: Update `CLAUDE.md`**

In the `Dependencies:` section, change line 19 from:

```
Since it is a ZIO project, it contains all ZIO-based dependencies (version `2.1.19`):
```

to:

```
Since it is a ZIO project, it contains all ZIO-based dependencies (version `2.1.26`):
```

And change the dependency bullet list (lines 21-25) from:

```
- zio
- zio-streams (optional)
- zio-test
- zio-test-sbt
- zio-jdbc - Postgres access
```

to:

```
- zio
- zio-streams (optional)
- zio-http - Reddit HTTP client (version `3.11.4`)
- zio-test
- zio-test-sbt
- zio-jdbc - Postgres access
```

- [ ] **Step 3: Verify dependency resolution and compilation**

Run: `sbt -batch compile`
Expected: `[success]` — no eviction warnings for `zio`/`zio-*` artifacts, no unresolved dependency errors.

- [ ] **Step 4: Commit**

```bash
git add build.sbt CLAUDE.md
git commit -m "$(cat <<'EOF'
Add zio-http dependency, bump ZIO to 2.1.26

zio-http 3.11.4 transitively depends on zio 2.1.26; bump the
project's pinned zio version to match rather than rely on sbt's
implicit eviction.
EOF
)"
```

---

### Task 2: RedditClient service (trait + handleResponse + Live + ZLayer)

**Files:**
- Test: `src/test/scala/ingest/RedditClientSpec.scala`
- Create: `src/main/scala/ingest/RedditClient.scala`

**Interfaces:**
- Consumes: `zio.http.Client` (from zio-http, added in Task 1), `zio.http.Client.default: ZLayer[Any, Throwable, Client]` (provided in Task 3).
- Produces:
  - `trait ingest.RedditClient { def fetchBest: Task[String] }`
  - `ingest.RedditClient.handleResponse(response: Response): Task[String]` — pure, no `Client` needed, used directly by the test below.
  - `ingest.RedditClient.live: ZLayer[Client, Nothing, RedditClient]`
  - `ingest.RedditClient.fetchBest: ZIO[RedditClient, Throwable, String]` — the accessor Task 3's `Main` calls.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ingest/RedditClientSpec.scala`:

```scala
package ingest

import zio._
import zio.http._
import zio.test._
import zio.test.Assertion._

object RedditClientSpec extends ZIOSpecDefault {
  def spec = suite("RedditClient")(
    test("handleResponse fails on a non-2xx response") {
      val response = Response(status = Status.TooManyRequests)
      assertZIO(RedditClient.handleResponse(response).exit)(
        fails(hasMessage(containsString("429"))),
      )
    },
  )
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt -batch "testOnly ingest.RedditClientSpec"`
Expected: FAIL to compile — `ingest.RedditClient` (and `handleResponse`) don't exist yet.

- [ ] **Step 3: Write `src/main/scala/ingest/RedditClient.scala`**

```scala
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt -batch "testOnly ingest.RedditClientSpec"`
Expected: PASS — 1 test passed.

- [ ] **Step 5: Compile the whole project**

Run: `sbt -batch compile`
Expected: `[success]`.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ingest/RedditClient.scala src/test/scala/ingest/RedditClientSpec.scala
git commit -m "$(cat <<'EOF'
Add RedditClient ingest service

Fetches https://www.reddit.com/best.json via zio-http's Client with
an explicit User-Agent (Reddit rejects the client's default UA),
a 30s timeout, and an explicit non-2xx status check. Returns the raw
response body; no parsing yet. handleResponse is unit-tested directly
against a canned Response.
EOF
)"
```

---

### Task 3: Wire `Main` to fetch and print

**Files:**
- Modify: `src/main/scala/Main.scala` (full file rewrite below)

**Interfaces:**
- Consumes: `ingest.RedditClient.fetchBest: ZIO[RedditClient, Throwable, String]`, `ingest.RedditClient.live: ZLayer[Client, Nothing, RedditClient]` (Task 2), `zio.http.Client.default: ZLayer[Any, Throwable, Client]` (zio-http).

- [ ] **Step 1: Replace the contents of `src/main/scala/Main.scala`**

```scala
import zio._
import zio.http.Client

import ingest.RedditClient

object App extends ZIOAppDefault {
  def run =
    RedditClient.fetchBest
      .flatMap(Console.printLine(_))
      .provide(RedditClient.live, Client.default)
}
```

- [ ] **Step 2: Compile**

Run: `sbt -batch compile`
Expected: `[success]`.

- [ ] **Step 3: Manual verification run**

Run: `sbt run`
Expected: the process prints a large raw JSON document to stdout (Reddit's `best.json` listing payload — starts with `{"kind": "Listing", "data": {...`) and exits with code 0. If instead you see a 429/blocked-content HTML page or a non-zero exit, the User-Agent header or URL is wrong — stop and re-check Task 2 Step 3 against the spec before continuing.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/Main.scala
git commit -m "$(cat <<'EOF'
Wire Main to fetch and print Reddit's best.json

First working slice of the ingest stage: fetches the raw listing
and prints it, verified manually via sbt run.
EOF
)"
```
