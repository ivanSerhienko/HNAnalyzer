The goal of the project is to gain real-life experience with Scala ZIO coding.

The project is a simple streaming application, ALL-IN-ONE - ingest-analyze-transform-report.
It fetches data from Reddit to analyze recent trends, discussions, etc.

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
- zio-http - Reddit HTTP client (version `3.11.4`)
- zio-test
- zio-test-sbt
- zio-jdbc - Postgres access

Scala version is `3.8.4`

Architecture overview:

The app is a continuously streaming ETL pipeline built on zio-streams:

- Ingest: polls Reddit's public JSON endpoints (e.g. reddit.com/*.json) on an interval. No
  OAuth/API credentials are used - only unauthenticated public endpoints.
- Analyze: derives recent trends/discussion signals from the ingested posts/comments.
- Transform: shapes the analysis output into a form suitable for persistence.
- Report: persists the transformed data into a Postgres database via zio-jdbc.

Environment/secrets:

- Config (e.g. Postgres connection details) is supplied via environment variables.
- No Reddit API credentials are needed (public JSON endpoints only).
- Postgres instance is hosted (not local); connection details are not yet configured.

Testing conventions:

- Integration tests that touch persistence connect to the hosted Postgres instance
  (connection details TBD - not yet configured).

Coding conventions:
- Use ZIO-style coding (effects, layers, ZStream for the ingest/pipeline, no side effects
  outside of ZIO effects).
