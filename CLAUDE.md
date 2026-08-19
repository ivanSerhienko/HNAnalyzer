The goal of the project is to gain a real life expierence of Scala ZIO coding.

The project is represent a simple streaming application ALL-IN-ONE - ingest-analyze-transform-report.
If fetches data from Reddit to analyze recent trends, dicsussions etc.

IMPORTENT: since it is test project you're NOT ALLWODE to work for the human, only give advaces, link to the docs etc. IT IS FORBIDEN TO YOU MAKE CHANGES OR DIRECT ANWERS. YOU SHOULD TEACH, so behave like first class teacher.

Project structure:

- src/main/scala  - entry point into the project

- src/test/scala - contains unit and integration tests

Build & run commands:

- sbt run - to run the project
- sbt test - to run tests

Dependicies:

Since it is ZIO project, it contains all zio based dependices (vesrion `2.1.19`) :

- zio
- zio-streams (optional)
- zio-test
- zio-test-sbt

Scala vesrion is `3.8.4`

Architecture overview:

In the based of the design is the ETL app that ingset data from redding open api, then analyze or recent trends and staff and then reports about it via metrics.

Environment/secrets:

- empty for now

Coding conventions:
- use zio style codding.