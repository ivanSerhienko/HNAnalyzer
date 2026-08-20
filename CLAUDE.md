The goal of the project is to gain real-life experience with Scala ZIO coding.

The project is a simple streaming application, ALL-IN-ONE - ingest-analyze-transform-report.
It fetches data from Hacker News to analyze recent trends, discussions, etc.

Project structure:

- src/main/scala  - entry point into the project

- src/test/scala - contains unit and integration tests

Build & run commands:

- sbt run - to run the project
- sbt test - to run tests

Dependencies:

Since it is a ZIO project, it contains all ZIO-based dependencies (version `2.1.26`):

- zio
- zio-streams (optional)
- zio-http - Hacker News HTTP client (version `3.11.4`)
- zio-test
- zio-test-sbt
- zio-jdbc - Postgres access

Scala version is `3.8.4`

Architecture overview:

The app is a continuously streaming ETL pipeline built on zio-streams:

- Ingest: polls Hacker News's public Firebase-backed API
  (hacker-news.firebaseio.com) on an interval - story listings (top/new/ask/show),
  individual items, and nested comment trees. No API key or OAuth is needed; the
  API is fully open and unauthenticated, with no published rate limit.
- Analyze: derives recent trends/discussion signals from the ingested posts/comments.
- Transform: shapes the analysis output into a form suitable for persistence.
- Report: persists the transformed data into a Postgres database via zio-jdbc.

Environment/secrets:

- Config (e.g. Postgres connection details) is supplied via environment variables.
- No Hacker News API credentials are needed (the public API requires no
  authentication).
- Postgres instance is hosted (not local); connection details are not yet configured.

Testing conventions:

- Integration tests that touch persistence connect to the hosted Postgres instance
  (connection details TBD - not yet configured).

Coding conventions:
- Use ZIO-style coding (effects, layers, ZStream for the ingest/pipeline, no side effects
  outside of ZIO effects).
