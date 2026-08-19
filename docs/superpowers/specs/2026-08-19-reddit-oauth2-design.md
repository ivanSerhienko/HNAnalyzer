# Reddit OAuth2 Support — Design

## Goal

Replace the unauthenticated `www.reddit.com/*.json` approach (confirmed
blocked by Reddit with a blanket 403 for non-browser clients — see
investigation in chat, corroborated by
[Reddit's own API docs](https://github.com/reddit-archive/reddit/wiki/API))
with Reddit's application-only OAuth2 flow (`client_credentials` grant),
so `ingest.RedditClient` can actually fetch data again.

## Why application-only (`client_credentials`) auth

RedditAnalyzer is an unattended background service reading public data —
it isn't acting on behalf of a specific logged-in Reddit user. Reddit's
`client_credentials` grant is documented as the fit for exactly this case:
it only needs the app's client ID/secret (obtained by registering a
"script" app at reddit.com/prefs/apps), never a Reddit account
username/password, and — per
[Reddit's OAuth2 wiki](https://github.com/reddit-archive/reddit/wiki/oauth2)
— is meant for "confidential clients... not acting on behalf of one or
more logged out users."

## Architecture / components

Two new files alongside the existing `ingest/RedditClient.scala`:

### `ingest/RedditCredentialsProvider.scala`

Owns sourcing credentials (from `REDDIT_CLIENT_ID_FILE`/
`REDDIT_CLIENT_SECRET_FILE`, see Secrets below) and keeping them
fresh — a separate responsibility from turning credentials into a token
(that's `RedditAuth`, below), matching the project's pattern of one
service per concern.

- `case class RedditCredentials(clientId: String, clientSecret: String)`
- `trait RedditCredentialsProvider { def current: UIO[RedditCredentials] }`
  — never fails; always returns the latest known-good value.
- `RedditCredentialsProvider.Live` — constructed from two file paths
  (see Secrets below). At construction, reads both files once via
  `ZIO.attemptBlockingIO(java.nio.file.Files.readString(path).trim)`;
  this initial read must succeed or layer construction fails (no
  known-good value exists yet). The result seeds a `Ref[RedditCredentials]`.
  A background fiber, forked with `ZIO.forkScoped` (so its lifetime is
  tied to the layer's `Scope` and it's cleaned up on shutdown), loops
  every 60 seconds (hardcoded, not configurable this slice): re-read both
  files; if the value changed, `ZIO.logInfo` and update the `Ref`; if the
  read fails, `ZIO.logWarning` with the failure reason and keep serving
  the previous value — a transient read failure never crashes the poller
  or the app.
- `RedditCredentialsProvider.live: ZLayer[Any, Throwable, RedditCredentialsProvider]`

### `ingest/RedditAuth.scala`

- `trait RedditAuth { def getToken: Task[String] }`
- `RedditAuth.CachedToken(token: String, expiresAt: Instant)` — the cached
  state.
- `RedditAuth.isValid(cached: Option[CachedToken], now: Instant): Boolean`
  — pure function: `true` only if `cached` is `Some` and its `expiresAt`
  is more than 60 seconds in the future (a small safety margin so a
  request doesn't start with a token that expires mid-flight).
  Independently unit-testable, same seam as `RedditClient.handleResponse`.
- `RedditAuth.Live` — depends on `Client` and `RedditCredentialsProvider`;
  holds a `Ref[Option[CachedToken]]`. `getToken`: read the ref; if
  `isValid` on the cached value, return its token; otherwise call
  `RedditCredentialsProvider.current` for the latest credentials, POST for
  a fresh token (see below), `ZIO.logInfo` the refresh (expiry only, never
  the token value), cache it, return it. Because credentials are re-read
  from the provider on every refresh (not captured once at construction),
  a rotated secret takes effect on the next token fetch after the
  provider's poll picks it up — no restart needed.
- `RedditAuth.live: ZLayer[Client & RedditCredentialsProvider, Nothing, RedditAuth]`
  initializes the `Ref` empty.

`RedditClient.Live` gains a `RedditAuth` dependency (alongside `Client`):
before issuing the data request, it calls `RedditAuth.getToken` and adds
`Authorization: Bearer <token>` to the request; the request host changes
from `www.reddit.com` to `oauth.reddit.com` (Reddit requires this host for
authenticated requests). `handleResponse` is unchanged.

### Fetching a token

```
POST https://www.reddit.com/api/v1/access_token
Authorization: Basic <base64(client_id:client_secret)>
Content-Type: application/x-www-form-urlencoded
User-Agent: RedditAnalyzer/0.1

grant_type=client_credentials
```

In zio-http terms (verified against 3.11.4 source):

```scala
Request
  .post(
    "https://www.reddit.com/api/v1/access_token",
    Body.fromString("grant_type=client_credentials")
      .contentType(MediaType.application.`x-www-form-urlencoded`),
  )
  .addHeader(Header.Authorization.Basic(clientId, clientSecret))
  .addHeader(userAgentHeader)
```

Response (JSON, decoded with zio-json — see Dependency below):

```json
{"access_token": "...", "token_type": "bearer", "expires_in": 3600, "scope": "..."}
```

```scala
final case class TokenResponse(access_token: String, expires_in: Long) derives JsonDecoder
```

`expiresAt` is computed as `now.plusSeconds(expires_in)` using `Clock`
(not `Instant.now()` directly), for the same testability reason `Clock`
is used in `isValid`.

## Dependency

Add `zio-json` `0.10.0` to `build.sbt` — its POM pins `zio` `2.1.26`
exactly, matching the project's existing pin with zero version
resolution conflict. Used only to decode the token endpoint's small,
fixed-shape response (`access_token`, `expires_in`); still no general
JSON parsing of Reddit's data payloads — that remains out of scope,
deferred to the Analyze stage.

No logging dependency is added. `RedditCredentialsProvider`, `RedditAuth`,
and `RedditClient` all use ZIO's built-in `ZIO.logInfo`/`logWarning`/
`logError`/`logDebug` — structured, leveled logging with zero extra
dependencies (its default console output format is already visible in
earlier `sbt run` failures in this project). **Rule for every log line
added in this slice: never log the actual client secret, client ID, or
bearer token value** — only metadata (timestamps, expiry, status codes,
file paths, byte counts).

## Secrets

Two environment variables — now holding **file paths**, not raw secret
values, so credentials can be rotated by updating the file's contents
without restarting the app (the standard Docker/Kubernetes-secrets
convention: mount a secret as a file, point an env var at its path):

- `REDDIT_CLIENT_ID_FILE`
- `REDDIT_CLIENT_SECRET_FILE`

Reading environment variables directly (as the original design in this
spec had it) cannot support rotation without a restart: a process's env
vars are fixed at OS process-start time on every mainstream platform and
never change afterward, no matter what the parent shell or orchestrator's
environment does later. A polled file is the mechanism that actually
allows a rotated secret to take effect live — see
`RedditCredentialsProvider` above.

If either env var is unset, or its file can't be read at startup,
`RedditCredentialsProvider.live`'s layer construction fails immediately
with a clear message, rather than surfacing as a confusing HTTP error
later.

Registering the Reddit app itself (a "script" type app at
reddit.com/prefs/apps) is a manual step only the user can do — out of
scope for code, covered separately with setup instructions.

## Data flow

`Main` → `RedditClient.fetchBest` → `RedditAuth.getToken` (cache hit, or:
`RedditCredentialsProvider.current` for the latest credentials, then a
fresh POST to `https://www.reddit.com/api/v1/access_token`) →
`RedditClient` issues `GET https://oauth.reddit.com/best.json` with
`Authorization: Bearer <token>` and the existing
`User-Agent: RedditAnalyzer/0.1` → `handleResponse` (unchanged, still
checks status and reads the body) → printed by `Main`.

Independently, `RedditCredentialsProvider`'s background fiber polls its
two files every 60 seconds for the life of the app, keeping the `Ref`
current regardless of whether a fetch is in flight.

## Error handling

- Missing env vars or unreadable credential files at startup: layer
  construction fails (see Secrets).
- A transient credential-file read failure during a poll: logged as a
  warning, previous credentials retained, app keeps running (see
  `RedditCredentialsProvider` above).
- Token endpoint request: wrapped in the same `.timeout(30.seconds)`
  policy as the data fetch; a non-2xx response fails clearly with a
  status-including message, the same shape of check as
  `RedditClient.handleResponse` but its own inline check in
  `RedditAuth.Live` — not shared code, since the two responses have
  different bodies and error semantics and the check itself is only a
  few lines (YAGNI: no premature shared abstraction for two call sites).
- Still no retries/backoff anywhere in this slice, consistent with the
  original ingest-client spec.
- **Explicitly not handled:** a cached token Reddit revokes early (before
  our tracked `expiresAt`) — e.g. manually revoked in the app dashboard.
  This surfaces as a normal `RedditClient` failure (401/403) on the next
  data request; there's no reactive "retry once with a fresh token" logic.
  That's a retry policy, deliberately deferred, same reasoning as the
  original ingest-client spec's stance on retries.

## Logging

Every action gets a leveled ZIO log line (never the secret/token values
themselves — see Dependency above):

| Component | Event | Level |
|---|---|---|
| `RedditCredentialsProvider` | Initial credentials loaded (file paths only) | INFO |
| `RedditCredentialsProvider` | Poll detects a changed credential, reloading | INFO |
| `RedditCredentialsProvider` | Poll failed to read a file, retaining previous value | WARNING |
| `RedditAuth` | Fetching a new OAuth token (cache miss/expired) | INFO |
| `RedditAuth` | Token refreshed, with its expiry time | INFO |
| `RedditAuth` | Serving a cached token (cache hit) | DEBUG (quiet by default; avoids per-request noise once the polling loop lands) |
| `RedditAuth` | Token fetch failed, with status | ERROR |
| `RedditClient` | Fetching `best.json` | INFO |
| `RedditClient` | Fetch succeeded, with byte count | INFO |

## Testing

Two new unit tests, same "extract the pure part" pattern as
`RedditClientSpec`:

- `RedditAuthSpec` covering `RedditAuth.isValid` directly — no
  `Client`/`Ref` faking needed:
  - `None` → `false`
  - `Some(CachedToken(_, expiresAt = far future))` → `true`
  - `Some(CachedToken(_, expiresAt = in the past))` → `false`

`RedditCredentialsProvider.Live` (file I/O, `Ref`, background fiber) and
`RedditAuth.Live`/`RedditClient.Live` (network I/O) are not unit-tested,
consistent with the project's existing stance on `Live` implementations.
Manual verification: run `sbt run` with real credentials set and confirm
JSON prints to stdout (this is also what finally closes out the previous
slice's still-open acceptance criterion — nobody has seen the success
path execute yet), plus a manual check that editing the credential files
while the app is running produces the "credentials rotated" log line
within 60 seconds.

## Documentation update

`CLAUDE.md`'s "Environment/secrets" section changes from "No Reddit API
credentials are needed" to documenting `REDDIT_CLIENT_ID_FILE`/
`REDDIT_CLIENT_SECRET_FILE` (file paths, not raw values — see Secrets);
the "Ingest" architecture bullet drops "No OAuth/API credentials are
used" and gains a line about credential rotation via polled files.

## Explicitly out of scope for this slice

- Reactive token-refresh-on-401/retry
- Retries/backoff in general (unchanged from the original ingest spec)
- The continuous polling/zio-streams loop
- JSON parsing of Reddit's actual data payloads (only the token
  response is decoded)
- Registering the Reddit app in the browser (manual step, covered
  separately from code)
- A configurable poll interval (hardcoded to 60 seconds)
- Any logging *library* (Logback/SLF4J) — plain ZIO logging only
- Eagerly invalidating a cached OAuth token when credentials rotate
  mid-lifetime — a still-valid cached token keeps being used until its
  own expiry; rotation only affects the *next* token fetch
