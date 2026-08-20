package storage

import zio._
import zio.jdbc._

final case class PostgresConfig(host: String, port: Int, database: String, user: String, password: String) {
  def props: Map[String, String] = Map("user" -> user, "password" -> password)
}

object PostgresConfig {
  // Reads the real OS environment directly, rather than going through ZIO's
  // `System` service - under zio-test's `ZIOSpecDefault`, that service
  // resolves to a mockable `TestSystem` with empty env vars by default, not
  // the live process environment.
  private def requireEnv(name: String): Task[String] =
    ZIO
      .attempt(Option(java.lang.System.getenv(name)))
      .someOrFail(
        new RuntimeException(s"Missing required environment variable: $name (copy .env.example to .env and fill it in)"),
      )

  val fromEnv: Task[PostgresConfig] =
    for {
      host  <- requireEnv("POSTGRES_HOST")
      portS <- requireEnv("POSTGRES_PORT")
      db    <- requireEnv("POSTGRES_DB")
      user  <- requireEnv("POSTGRES_USER")
      pass  <- requireEnv("POSTGRES_PASSWORD")
    } yield PostgresConfig(host, portS.toInt, db, user, pass)
}

object Db {
  private val poolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  val pool: ZLayer[Any, Throwable, ZConnectionPool] =
    poolConfig >>> ZLayer.fromZIO(PostgresConfig.fromEnv).flatMap { env =>
      val cfg = env.get[PostgresConfig]
      ZConnectionPool.postgres(cfg.host, cfg.port, cfg.database, cfg.props)
    }
}
