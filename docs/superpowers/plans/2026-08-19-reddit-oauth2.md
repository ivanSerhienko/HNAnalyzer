# Reddit OAuth2 Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unauthenticated (now Reddit-blocked) `www.reddit.com/*.json` fetch with Reddit's OAuth2 application-only (`client_credentials`) flow, with file-based credentials that can rotate without an app restart, and structured logging across the ingest components.

**Architecture:** A new `RedditCredentialsProvider` service sources and polls credential files. A new `RedditAuth` service turns those credentials into a cached, expiry-aware OAuth2 bearer token. `RedditClient` is modified to depend on `RedditAuth`, target `oauth.reddit.com`, and add the `Authorization: Bearer` header. `Main` wires all three new/changed layers together.

**Tech Stack:** Scala 3.8.4, ZIO 2.1.26, zio-http 3.11.4, zio-json 0.10.0, sbt 1.12.8.

**Spec:** `docs/superpowers/specs/2026-08-19-reddit-oauth2-design.md`

## Global Constraints

- `zio-json` version is exactly `0.10.0` (its POM pins `zio` `2.1.26`, matching this project's pin with zero resolution conflict).
- No new logging dependency — every log line uses ZIO's built-in `ZIO.logInfo`/`ZIO.logWarning`/`ZIO.logError`/`ZIO.logDebug`.
- **No log line may ever contain the actual client secret, client ID, or bearer token value** — only metadata (timestamps, expiry, status codes, file paths, byte counts).
- Credentials are sourced from `REDDIT_CLIENT_ID_FILE`/`REDDIT_CLIENT_SECRET_FILE` env vars, which hold **file paths**, not raw values. The files are polled every 60 seconds (hardcoded, not configurable this slice).
- A transient credential-file read failure during a poll is logged as a warning; the previously-cached credentials keep being served — the app never crashes from a poll failure.
- `RedditAuth.getToken` has its own `.timeout(30.seconds)` around the token-fetch-and-parse sequence; `RedditClient.fetchBest`'s data request has its own separate `.timeout(30.seconds)` around the request-and-validate sequence. **These are two independent timeout budgets, not nested** — `fetchBest`'s timeout wraps only the `best.json` request, not the call to `auth.getToken`, so a fresh token fetch and the data fetch each get their own full 30s rather than competing for one shared budget. (This is a plan-level decision filling a gap the spec didn't spell out explicitly — the spec's stance that the data fetch's timeout covers "the whole fetch-and-validate sequence" refers to the original single-service design; composing two independently-timed-out services is the correct generalization.)
- No retries/backoff, no reactive token-refresh-on-401, no JSON parsing of Reddit's actual data payloads (only the token response), no configurable poll interval, no eager cache invalidation on credential rotation — all explicitly out of scope per the spec.
- `RedditClient.handleResponse` (existing, tested) is unchanged by this plan.

---

### Task 1: Add zio-json dependency

**Files:**
- Modify: `build.sbt` (full file rewrite below)

**Interfaces:**
- Produces: `zio-json` on the classpath at `0.10.0`, available to Task 3's `import zio.json._`.

- [ ] **Step 1: Update `build.sbt`**

Replace the full contents of `build.sbt` with:

```scala
ThisBuild / scalaVersion := "3.8.4"

val zioVersion = "2.1.26"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"          % zioVersion,
  "dev.zio" %% "zio-streams"  % zioVersion % Optional,
  "dev.zio" %% "zio-http"     % "3.11.4",
  "dev.zio" %% "zio-json"     % "0.10.0",
  "dev.zio" %% "zio-test"     % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test
)
```

- [ ] **Step 2: Verify dependency resolution and compilation**

Run: `sbt -batch compile`
Expected: `[success]` — no eviction warnings for `zio`/`zio-*` artifacts, no unresolved dependency errors.

- [ ] **Step 3: Commit**

```bash
git add build.sbt
git commit -m "$(cat <<'EOF'
Add zio-json dependency

Needed to decode Reddit's OAuth2 token endpoint response
(access_token, expires_in) in the upcoming RedditAuth service.
EOF
)"
```

---

### Task 2: RedditCredentialsProvider (file-based credentials, polled for rotation)

**Files:**
- Create: `src/main/scala/ingest/RedditCredentialsProvider.scala`

**Interfaces:**
- Consumes: nothing from earlier tasks (uses only `zio` core, already present).
- Produces:
  - `final case class ingest.RedditCredentials(clientId: String, clientSecret: String)`
  - `trait ingest.RedditCredentialsProvider { def current: UIO[RedditCredentials] }`
  - `ingest.RedditCredentialsProvider.live: ZLayer[Any, Throwable, RedditCredentialsProvider]` — Task 3's `RedditAuth.live` depends on this.

No automated test for this task (per spec: file I/O, `Ref` mutation, and a background fiber aren't unit-tested, consistent with the project's stance on `Live` implementations). Deliverable is verified by compilation.

- [ ] **Step 1: Create `src/main/scala/ingest/RedditCredentialsProvider.scala`**

```scala
package ingest

import zio._

final case class RedditCredentials(clientId: String, clientSecret: String)

trait RedditCredentialsProvider {
  def current: UIO[RedditCredentials]
}

object RedditCredentialsProvider {

  private val pollInterval = 60.seconds

  private def readEnvPath(name: String): Task[String] =
    System.env(name).flatMap {
      case Some(value) => ZIO.succeed(value)
      case None        => ZIO.fail(new RuntimeException(s"$name environment variable is not set"))
    }

  private def readFile(path: String): Task[String] =
    ZIO.attemptBlockingIO(java.nio.file.Files.readString(java.nio.file.Path.of(path)).trim)

  private def loadCredentials(clientIdPath: String, clientSecretPath: String): Task[RedditCredentials] =
    for {
      clientId     <- readFile(clientIdPath)
      clientSecret <- readFile(clientSecretPath)
    } yield RedditCredentials(clientId, clientSecret)

  final case class Live(ref: Ref[RedditCredentials]) extends RedditCredentialsProvider {
    override def current: UIO[RedditCredentials] = ref.get
  }

  val live: ZLayer[Any, Throwable, RedditCredentialsProvider] =
    ZLayer.scoped {
      for {
        clientIdPath     <- readEnvPath("REDDIT_CLIENT_ID_FILE")
        clientSecretPath <- readEnvPath("REDDIT_CLIENT_SECRET_FILE")
        initial          <- loadCredentials(clientIdPath, clientSecretPath)
        _                <- ZIO.logInfo(
                               s"Loaded Reddit credentials (REDDIT_CLIENT_ID_FILE=$clientIdPath, " +
                                 s"REDDIT_CLIENT_SECRET_FILE=$clientSecretPath)",
                             )
        ref              <- Ref.make(initial)
        poll              = loadCredentials(clientIdPath, clientSecretPath)
                               .flatMap { reloaded =>
                                 ref.get.flatMap { current =>
                                   if (reloaded != current)
                                     ZIO.logInfo("Reddit credentials rotated, reloading") *> ref.set(reloaded)
                                   else
                                     ZIO.unit
                                 }
                               }
                               .catchAll { error =>
                                 ZIO.logWarning(
                                   s"Failed to read Reddit credentials file, retaining previous value: ${error.getMessage}",
                                 )
                               }
        _                <- (ZIO.sleep(pollInterval) *> poll).forever.forkScoped
      } yield Live(ref)
    }
}
```

- [ ] **Step 2: Compile**

Run: `sbt -batch compile`
Expected: `[success]`, no errors in `ingest/RedditCredentialsProvider.scala`.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/ingest/RedditCredentialsProvider.scala
git commit -m "$(cat <<'EOF'
Add RedditCredentialsProvider for file-based, rotatable secrets

Sources REDDIT_CLIENT_ID_FILE/REDDIT_CLIENT_SECRET_FILE and polls
them every 60s so a rotated secret takes effect without an app
restart. A transient read failure during a poll is logged and the
previous value keeps being served; only the initial read at startup
is fatal.
EOF
)"
```

---

### Task 3: RedditAuth (OAuth2 token fetch/cache)

**Files:**
- Test: `src/test/scala/ingest/RedditAuthSpec.scala`
- Create: `src/main/scala/ingest/RedditAuth.scala`

**Interfaces:**
- Consumes: `zio.http.Client` (from zio-http), `ingest.RedditCredentialsProvider` with `.current: UIO[RedditCredentials]` (Task 2), `zio.json._` (Task 1).
- Produces:
  - `trait ingest.RedditAuth { def getToken: Task[String] }`
  - `ingest.RedditAuth.CachedToken(token: String, expiresAt: java.time.Instant)`
  - `ingest.RedditAuth.isValid(cached: Option[CachedToken], now: java.time.Instant): Boolean` — pure, used directly by the test below.
  - `ingest.RedditAuth.live: ZLayer[Client & RedditCredentialsProvider, Nothing, RedditAuth]` — Task 4's `RedditClient.live` depends on this.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ingest/RedditAuthSpec.scala`:

```scala
package ingest

import zio._
import zio.test._

import java.time.Instant

object RedditAuthSpec extends ZIOSpecDefault {
  def spec = suite("RedditAuth")(
    test("isValid is false when there is no cached token") {
      assertTrue(!RedditAuth.isValid(None, Instant.now()))
    },
    test("isValid is true when the cached token expires well in the future") {
      val now    = Instant.now()
      val cached = Some(RedditAuth.CachedToken("token", now.plusSeconds(3600)))
      assertTrue(RedditAuth.isValid(cached, now))
    },
    test("isValid is false when the cached token has already expired") {
      val now    = Instant.now()
      val cached = Some(RedditAuth.CachedToken("token", now.minusSeconds(10)))
      assertTrue(!RedditAuth.isValid(cached, now))
    },
  )
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt -batch "testOnly ingest.RedditAuthSpec"`
Expected: FAIL to compile — `ingest.RedditAuth` doesn't exist yet.

- [ ] **Step 3: Write `src/main/scala/ingest/RedditAuth.scala`**

```scala
package ingest

import zio._
import zio.http._
import zio.json._

import java.time.Instant

trait RedditAuth {
  def getToken: Task[String]
}

object RedditAuth {

  private val tokenUrl = "https://www.reddit.com/api/v1/access_token"

  private val userAgentHeader: Header.UserAgent =
    Header.UserAgent(
      Header.UserAgent.ProductOrComment.Product("RedditAnalyzer", Some("0.1")),
    )

  private val validityMarginSeconds = 60L

  final case class CachedToken(token: String, expiresAt: Instant)

  final case class TokenResponse(access_token: String, expires_in: Long) derives JsonDecoder

  def isValid(cached: Option[CachedToken], now: Instant): Boolean =
    cached.exists(c => now.plusSeconds(validityMarginSeconds).isBefore(c.expiresAt))

  final case class Live(client: Client, credentials: RedditCredentialsProvider, ref: Ref[Option[CachedToken]])
      extends RedditAuth {

    private def fetchToken: Task[CachedToken] =
      for {
        creds    <- credentials.current
        _        <- ZIO.logInfo("Fetching new Reddit OAuth token")
        response <- client.batched(
                      Request
                        .post(
                          tokenUrl,
                          Body
                            .fromString("grant_type=client_credentials")
                            .contentType(MediaType.application.`x-www-form-urlencoded`),
                        )
                        .addHeader(Header.Authorization.Basic(creds.clientId, creds.clientSecret))
                        .addHeader(userAgentHeader),
                    )
        body     <- if (response.status.isSuccess) response.body.asString
                    else
                      ZIO.logError(s"Reddit token request failed: ${response.status.code} ${response.status}") *>
                        ZIO.fail(
                          new RuntimeException(
                            s"Reddit token request returned ${response.status.code} ${response.status}: request failed",
                          ),
                        )
        parsed   <- ZIO.fromEither(body.fromJson[TokenResponse]).mapError(msg => new RuntimeException(msg))
        now      <- Clock.instant
        expiresAt = now.plusSeconds(parsed.expires_in)
        _        <- ZIO.logInfo(s"Reddit OAuth token refreshed, expires at $expiresAt")
      } yield CachedToken(parsed.access_token, expiresAt)

    override def getToken: Task[String] =
      (ref.get zip Clock.instant)
        .flatMap { case (cached, now) =>
          cached match {
            case Some(c) if isValid(cached, now) =>
              ZIO.logDebug("Using cached Reddit OAuth token").as(c.token)
            case _ =>
              fetchToken.flatMap(fresh => ref.set(Some(fresh)).as(fresh.token))
          }
        }
        .timeout(30.seconds)
        .someOrFail(new RuntimeException("Reddit OAuth token request timed out after 30s"))
  }

  val live: ZLayer[Client & RedditCredentialsProvider, Nothing, RedditAuth] =
    ZLayer.fromZIO {
      for {
        client      <- ZIO.service[Client]
        credentials <- ZIO.service[RedditCredentialsProvider]
        ref         <- Ref.make[Option[CachedToken]](None)
      } yield Live(client, credentials, ref)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt -batch "testOnly ingest.RedditAuthSpec"`
Expected: PASS — 3 tests passed.

- [ ] **Step 5: Compile the whole project**

Run: `sbt -batch compile`
Expected: `[success]`.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ingest/RedditAuth.scala src/test/scala/ingest/RedditAuthSpec.scala
git commit -m "$(cat <<'EOF'
Add RedditAuth OAuth2 client_credentials token service

Fetches and caches a bearer token from Reddit's access_token
endpoint, refreshing when the cached token is missing or within 60s
of expiry. Credentials are re-read from RedditCredentialsProvider on
every refresh, so a rotated secret takes effect on the next token
fetch. isValid is unit-tested directly against canned inputs.
EOF
)"
```

---

### Task 4: Modify RedditClient to use OAuth2

**Files:**
- Modify: `src/main/scala/ingest/RedditClient.scala` (full file rewrite below)

**Interfaces:**
- Consumes: `ingest.RedditAuth` with `.getToken: Task[String]` (Task 3), `zio.http.Client`.
- Produces:
  - `ingest.RedditClient.live: ZLayer[Client & RedditAuth, Nothing, RedditClient]` (type changed from the prior slice's `ZLayer[Client, Nothing, RedditClient]` — now also requires `RedditAuth`). Task 5's `Main` consumes this.
  - `ingest.RedditClient.fetchBest: ZIO[RedditClient, Throwable, String]` — unchanged signature.
  - `ingest.RedditClient.handleResponse(response: Response): Task[String]` — **unchanged**, do not modify its body or signature; `RedditClientSpec` (existing, from the prior slice) tests it directly and must keep passing unmodified.

- [ ] **Step 1: Replace the contents of `src/main/scala/ingest/RedditClient.scala`**

```scala
package ingest

import zio._
import zio.http._

trait RedditClient {
  def fetchBest: Task[String]
}

object RedditClient {

  private val bestUrl = "https://oauth.reddit.com/best.json"

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

  final case class Live(client: Client, auth: RedditAuth) extends RedditClient {
    override def fetchBest: Task[String] =
      for {
        _     <- ZIO.logInfo("Fetching Reddit best.json")
        token <- auth.getToken
        body  <- (for {
                   response <- client.batched(
                                 Request
                                   .get(bestUrl)
                                   .addHeader(userAgentHeader)
                                   .addHeader(Header.Authorization.Bearer(token)),
                               )
                   result   <- handleResponse(response)
                 } yield result)
                   .timeout(30.seconds)
                   .someOrFail(new RuntimeException("Request to Reddit timed out after 30s"))
        _     <- ZIO.logInfo(s"Fetched Reddit best.json successfully (${body.length} bytes)")
      } yield body
  }

  val live: ZLayer[Client & RedditAuth, Nothing, RedditClient] =
    ZLayer.fromFunction(Live.apply _)

  def fetchBest: ZIO[RedditClient, Throwable, String] =
    ZIO.serviceWithZIO[RedditClient](_.fetchBest)
}
```

- [ ] **Step 2: Run the existing RedditClientSpec to confirm it still passes unmodified**

Run: `sbt -batch "testOnly ingest.RedditClientSpec"`
Expected: PASS — 1 test passed (this test only exercises `handleResponse`, which this change didn't touch).

- [ ] **Step 3: Compile the whole project**

Run: `sbt -batch compile`
Expected: `[success]`.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/ingest/RedditClient.scala
git commit -m "$(cat <<'EOF'
Switch RedditClient to OAuth2 authenticated requests

Depends on RedditAuth for a bearer token, targets oauth.reddit.com
instead of www.reddit.com per Reddit's requirement for authenticated
requests, and adds fetch-start/fetch-success logging. handleResponse
and its existing test are unchanged.
EOF
)"
```

---

### Task 5: Wire Main and update CLAUDE.md

**Files:**
- Modify: `src/main/scala/Main.scala` (full file rewrite below)
- Modify: `CLAUDE.md:19` (dependency list), `CLAUDE.md:34-35` (Ingest bullet), `CLAUDE.md:42-43` (Environment/secrets bullets)

**Interfaces:**
- Consumes: `ingest.RedditClient.fetchBest`, `ingest.RedditClient.live` (Task 4); `ingest.RedditAuth.live` (Task 3); `ingest.RedditCredentialsProvider.live` (Task 2); `zio.http.Client.default` (zio-http, already in place).

- [ ] **Step 1: Replace the contents of `src/main/scala/Main.scala`**

```scala
import zio._
import zio.http.Client

import ingest.{RedditAuth, RedditClient, RedditCredentialsProvider}

object App extends ZIOAppDefault {
  def run =
    RedditClient.fetchBest
      .flatMap(Console.printLine(_))
      .provide(
        RedditClient.live,
        RedditAuth.live,
        RedditCredentialsProvider.live,
        Client.default,
      )
}
```

- [ ] **Step 2: Update `CLAUDE.md`**

Change line 19's dependency bullet list from:

```
- zio
- zio-streams (optional)
- zio-http - Reddit HTTP client (version `3.11.4`)
- zio-test
- zio-test-sbt
- zio-jdbc - Postgres access
```

to:

```
- zio
- zio-streams (optional)
- zio-http - Reddit HTTP client (version `3.11.4`)
- zio-json - decodes the Reddit OAuth2 token response (version `0.10.0`)
- zio-test
- zio-test-sbt
- zio-jdbc - Postgres access
```

Change the Ingest bullet (lines 34-35) from:

```
- Ingest: polls Reddit's public JSON endpoints (e.g. reddit.com/*.json) on an interval. No
  OAuth/API credentials are used - only unauthenticated public endpoints.
```

to:

```
- Ingest: polls Reddit's public JSON endpoints (e.g. reddit.com/*.json) on an interval,
  authenticated via Reddit's OAuth2 application-only (client_credentials) flow. Client
  credentials are sourced from files and polled every 60s, so a rotated secret takes
  effect without restarting the app (see Environment/secrets).
```

Change the Environment/secrets bullets (lines 42-43) from:

```
- Config (e.g. Postgres connection details) is supplied via environment variables.
- No Reddit API credentials are needed (public JSON endpoints only).
```

to:

```
- Config (e.g. Postgres connection details) is supplied via environment variables.
- Reddit OAuth2 credentials are supplied via REDDIT_CLIENT_ID_FILE and
  REDDIT_CLIENT_SECRET_FILE environment variables, each pointing to a file containing
  the raw client ID/secret value (not the value itself in the env var). The files are
  polled every 60 seconds so a rotated secret takes effect without an app restart.
```

- [ ] **Step 3: Compile**

Run: `sbt -batch compile`
Expected: `[success]`.

- [ ] **Step 4: Manual verification run**

This requires real Reddit app credentials and two local files. Before running:
1. Register a "script" type app at reddit.com/prefs/apps (manual, browser-based — not part of this plan's code).
2. Create two files containing the raw client ID and client secret respectively (no trailing content needed beyond the value itself — the code trims whitespace).
3. Set `REDDIT_CLIENT_ID_FILE` and `REDDIT_CLIENT_SECRET_FILE` to those two files' paths.

Run: `sbt run`
Expected: log lines for "Loaded Reddit credentials...", "Fetching Reddit best.json", "Fetching new Reddit OAuth token", "Reddit OAuth token refreshed, expires at ...", "Fetched Reddit best.json successfully (N bytes)", then the raw JSON body printed to stdout (starts with `{"kind": "Listing", "data": {...`), exit code 0.

If any step fails, capture the exact error and stop — do not guess at fixes; report back with the failure for the controller to diagnose against the spec.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/Main.scala CLAUDE.md
git commit -m "$(cat <<'EOF'
Wire OAuth2 layers into Main, update CLAUDE.md

Main now provides RedditCredentialsProvider.live and RedditAuth.live
alongside the existing RedditClient.live and Client.default.
CLAUDE.md's dependency list, Ingest description, and
Environment/secrets section are updated to reflect OAuth2 and
file-based, rotatable credentials.
EOF
)"
```
