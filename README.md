# HNAnalyzer

A Scala ZIO learning project that continuously ingests Hacker News activity
(top/new/ask/show stories and their top-level comments) and analyzes recent
trends and discussions in real time.

The pipeline follows a medallion architecture, all persisted to Postgres:

- **Bronze** - raw, verbatim capture of every Hacker News API response.
- **Silver** - normalized story and comment tables.
- **Gold** - trending keywords, story velocity, and domain trend analysis.

## Running locally

1. Copy `.env.example` to `.env` and fill in real values.
2. Start Postgres: `docker compose up -d`
3. Apply the schema in `db/init/` if the container's data volume already
   existed before `.env` was set up (it only auto-applies on a fresh volume).
4. Run the app: `make run`
5. Run the tests: `make test`

See `CLAUDE.md` for architecture and coding conventions.
