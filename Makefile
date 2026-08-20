.PHONY: run test

run:
	set -a; . ./.env; set +a; sbt run

test:
	set -a; . ./.env; set +a; sbt test
