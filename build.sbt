ThisBuild / organization := "ma.chinespirit"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := "3.8.2"
ThisBuild / scalacOptions ++= Seq("-deprecation")
ThisBuild / homepage := Some(url("https://github.com/lbialy/parlance"))
ThisBuild / licenses += (
  "Apache-2.0",
  url(
    "https://opensource.org/licenses/Apache-2.0"
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/lbialy/parlance"),
    "scm:git:git@github.com:lbialy/parlance.git",
    Some("scm:git:git@github.com:lbialy/parlance.git")
  )
)
ThisBuild / developers := List(
  Developer(
    id = "lbialy",
    name = "Lukasz Bialy",
    email = "lukasz@chinespirit.ma",
    url = url("https://chinespirit.ma")
  )
)
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publish / skip := true
ThisBuild / coverageExcludedPackages := ""
ThisBuild / coverageFailOnMinimum := false

addCommandAlias("fmt", "scalafmtAll")

val testcontainersVersion = "0.41.4"
val circeVersion = "0.14.10"
val munitVersion = "1.1.0"
val postgresDriverVersion = "42.7.4"

lazy val root = project
  .in(file("."))
  .aggregate(parlance, parlancePg, parlanceMigrate, parlanceExamples)

lazy val parlance = project
  .in(file("parlance"))
  .settings(
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % postgresDriverVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersVersion % Test,
      "com.mysql" % "mysql-connector-j" % "9.0.0" % Test,
      "com.h2database" % "h2" % "2.3.232" % Test,
      "com.dimafeng" %% "testcontainers-scala-oracle-xe" % testcontainersVersion % Test,
      "com.oracle.database.jdbc" % "ojdbc11" % "21.9.0.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-clickhouse" % testcontainersVersion % Test,
      "com.clickhouse" % "clickhouse-jdbc" % "0.6.0" % Test classifier "http",
      "org.xerial" % "sqlite-jdbc" % "3.46.1.3" % Test
    )
  )

lazy val parlanceMigrate = project
  .in(file("parlance-migrate"))
  .dependsOn(parlance)
  .settings(
    name := "parlance-migrate",
    publish / skip := false,
    Test / fork := true,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.h2database" % "h2" % "2.3.232" % Test
    )
  )

lazy val parlanceExamples = project
  .in(file("parlance-examples"))
  .dependsOn(parlance, parlancePg, parlanceMigrate)
  .settings(
    name := "parlance-examples",
    Test / fork := true,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % postgresDriverVersion % Test
    )
  )

lazy val parlancePg = project
  .in(file("parlance-pg"))
  .dependsOn(parlance)
  .settings(
    name := "parlance-pg",
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % postgresDriverVersion % "provided",
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "io.circe" %% "circe-core" % circeVersion % Test,
      "io.circe" %% "circe-parser" % circeVersion % Test,
      "org.scala-lang.modules" %% "scala-xml" % "2.3.0" % Test
    )
  )
