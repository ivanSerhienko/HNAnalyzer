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

New package member `ingest/RedditAuth.scala`, alongside the existing
`ingest/RedditClient.scala`:

- `trait RedditAuth { def getToken: Task[String] }`
- `RedditAuth.CachedToken(token: String, expiresAt: Instant)` — the cached
  state.
- `RedditAuth.isValid(cached: Option[CachedToken], now: Instant): Boolean`
  — pure function: `true` only if `cached` is `Some` and its `expiresAt`
  is more than 60 seconds in the future (a small safety margin so a
  request doesn't start with a token that expires mid-flight).
  Independently unit-testable, same seam as `RedditClient.handleResponse`.
- `RedditAuth.Live` — holds a `Ref[Option[CachedToken]]` plus the client
  ID/secret. `getToken`: read the ref; if `isValid` on the cached value,
  return its token; otherwise POST for a fresh one (see below), cache it,
  return it.
- `RedditAuth.live: ZLayer[Client, Throwable, RedditAuth]` — reads
  `REDDIT_CLIENT_ID`/`REDDIT_CLIENT_SECRET` from the environment at
  construction (see Secrets below) and initializes the `Ref` empty.

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

## Secrets

Two new environment variables, read via `zio.System.env` (no new config
library — consistent with CLAUDE.md's existing "config via environment
variables" convention):

- `REDDIT_CLIENT_ID`
- `REDDIT_CLIENT_SECRET`

If either is unset, `RedditAuth.live`'s layer construction fails
immediately with a clear message (e.g.
`"REDDIT_CLIENT_ID environment variable is not set"`), rather than
surfacing as a confusing HTTP error later.

Registering the Reddit app itself (a "script" type app at
reddit.com/prefs/apps) is a manual step only the user can do — out of
scope for code, covered separately with setup instructions.

## Data flow

`Main` → `RedditClient.fetchBest` → `RedditAuth.getToken` (cache hit, or a
fresh POST to `https://www.reddit.com/api/v1/access_token`) →
`RedditClient` issues `GET https://oauth.reddit.com/best.json` with
`Authorization: Bearer <token>` and the existing
`User-Agent: RedditAnalyzer/0.1` → `handleResponse` (unchanged, still
checks status and reads the body) → printed by `Main`.

## Error handling

- Missing env vars: layer construction fails at startup (see Secrets).
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

## Testing

One new unit test (`RedditAuthSpec`) covering `RedditAuth.isValid`
directly — no `Client`/`Ref` faking needed, same pattern as
`RedditClientSpec`:

- `None` → `false`
- `Some(CachedToken(_, expiresAt = far future))` → `true`
- `Some(CachedToken(_, expiresAt = in the past))` → `false`

The actual token-fetch network call and `Ref` mutation are not
unit-tested, consistent with `RedditClient.Live.fetchBest` today.
Manual verification: run `sbt run` with real credentials set and confirm
JSON prints to stdout (this is also what finally closes out the previous
slice's still-open acceptance criterion — nobody has seen the success
path execute yet).

## Documentation update

`CLAUDE.md`'s "Environment/secrets" section changes from "No Reddit API
credentials are needed" to documenting `REDDIT_CLIENT_ID`/
`REDDIT_CLIENT_SECRET`; the "Ingest" architecture bullet drops "No
OAuth/API credentials are used."

## Explicitly out of scope for this slice

- Reactive token-refresh-on-401/retry
- Retries/backoff in general (unchanged from the original ingest spec)
- The continuous polling/zio-streams loop
- JSON parsing of Reddit's actual data payloads (only the token
  response is decoded)
- Registering the Reddit app in the browser (manual step, covered
  separately from code)
