import com.typesafe.tools.mima.core.{Problem, ProblemFilters}

enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-kafka"

val Nightly = sys.env.get("TRAVIS_EVENT_TYPE").contains("cron")

val Scala211 = "2.11.12"
val Scala212 = "2.12.10"
val Scala213 = "2.13.1"
val akkaVersion = if (Nightly) "2.6.0" else "2.5.23"
val AkkaBinaryVersion = if (Nightly) "2.6" else "2.5"
val kafkaVersion = "2.4.0"
val embeddedKafkaVersion = kafkaVersion
val embeddedKafka = "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion
// this depends on Kafka, and should be upgraded to such latest version
// that depends on the same Kafka version, as is defined above
val embeddedKafkaSchemaRegistry = "5.3.2"
val kafkaVersionForDocs = "24"
val scalatestVersion = "3.0.8"
val testcontainersVersion = "1.12.4"
val slf4jVersion = "1.7.26"
val confluentAvroSerializerVersion = "5.3.2"

val repoSettings = Seq(
  publishTo := {
    val nexus = "https://nexus.waylay.io"
    Some("Waylay releases repo" at nexus + "/repository/maven-releases")
  },
  // TODO remove below hack when on sbt 1.1.x and we don't need the watch service any more
  // https://github.com/sbt/sbt/issues/3519#issuecomment-331395080
  // https://github.com/swoval/MacOSXWatchService/issues/19
  updateOptions := updateOptions.value.withGigahorse(false)
)

// Allows to silence scalac compilation warnings selectively by code block or file path
// This is only compile time dependency, therefore it does not affect the generated bytecode
// https://github.com/ghik/silencer
val silencer = {
  val Version = "1.4.4"
  Seq(
    compilerPlugin("com.github.ghik" % "silencer-plugin" % Version cross CrossVersion.patch),
    "com.github.ghik" % "silencer-lib" % Version % Provided cross CrossVersion.patch
  )
}

resolvers in ThisBuild ++= Seq(
  // for Jupiter interface (JUnit 5)
  Resolver.jcenterRepo
)

TaskKey[Unit]("verifyCodeFmt") := {
  javafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Java code found. Please run 'javafmtAll' and commit the reformatted code"
    )
  }
  scalafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Scala code found. Please run 'scalafmtAll' and commit the reformatted code"
    )
  }
  (Compile / scalafmtSbtCheck).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted sbt code found. Please run 'scalafmtSbt' and commit the reformatted code"
    )
  }
}

addCommandAlias("verifyCodeStyle", "headerCheck; verifyCodeFmt")

addCommandAlias("verifyDocs", ";+doc ;unidoc ;docs/paradoxBrowse")

val commonSettings = repoSettings ++ Def.settings(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  organizationHomepage := Some(url("https://www.lightbend.com/")),
  homepage := Some(url("https://doc.akka.io/docs/alpakka-kafka/current")),
  scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka-kafka"), "git@github.com:akka/alpakka-kafka.git")),
  developers += Developer("contributors",
                          "Contributors",
                          "https://gitter.im/akka/dev",
                          url("https://github.com/akka/alpakka-kafka/graphs/contributors")),
  startYear := Some(2014),
  licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  description := "Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.",
  crossScalaVersions := Seq(Scala212, Scala211, Scala213).filterNot(_ == Scala211 && Nightly),
  scalaVersion := Scala212,
  crossVersion := CrossVersion.binary,
  javacOptions ++= Seq(
      "-Xlint:deprecation"
    ),
  scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8", // yes, this is 2 args
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    ) ++ {
      if (scalaBinaryVersion.value == "2.13") Seq.empty
      else Seq("-Yno-adapted-args", "-Xfuture")
    } ++ {
      if (insideCI.value && !Nightly) Seq("-Xfatal-warnings")
      else Seq.empty
    },
  Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
      "-doc-title",
      "Alpakka Kafka",
      "-doc-version",
      version.value,
      "-sourcepath",
      (baseDirectory in ThisBuild).value.toString,
      "-skip-packages",
      "akka.pattern" // for some reason Scaladoc creates this
    ) ++ {
      scalaBinaryVersion.value match {
        case "2.12" | "2.13" =>
          Seq(
            "-doc-source-url", {
              val branch = if (isSnapshot.value) "master" else s"v${version.value}"
              s"https://github.com/akka/alpakka-kafka/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
            },
            "-doc-canonical-base-url",
            "https://doc.akka.io/api/alpakka-kafka/current/"
          )
        case "2.11" =>
          Seq(
            "-doc-source-url", {
              val branch = if (isSnapshot.value) "master" else s"v${version.value}"
              s"https://github.com/akka/alpakka-kafka/tree/${branch}€{FILE_PATH}.scala#L1"
            }
          )
      }
    },
  Compile / doc / scalacOptions -= "-Xfatal-warnings",
  // show full stack traces and test case durations
  testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  // https://github.com/maichler/sbt-jupiter-interface#framework-options
  // -a Show stack traces and exception class name for AssertionErrors.
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -q Suppress stdout for successful tests.
  // -s Try to decode Scala names in stack traces and test names.
  testOptions += Tests.Argument(jupiterTestFramework, "-a", "-v", "-q", "-s"),
  scalafmtOnCompile := true,
  headerLicense := Some(
      HeaderLicense.Custom(
        """|Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
         |Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
         |""".stripMargin
      )
    ),
//  bintrayOrganization := Some("akka"),
//  bintrayPackage := "alpakka-kafka",
//  bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven"),
  projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
)

lazy val `alpakka-kafka` =
  project
    .in(file("."))
    .enablePlugins(ScalaUnidocPlugin)
    .disablePlugins(SitePlugin, MimaPlugin)
    .settings(commonSettings)
    .settings(
      skip in publish := true,
      ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, testkit),
      onLoadMessage :=
        """
            |** Welcome to the Alpakka Kafka connector! **
            |
            |The build has three main modules:
            |  core - the Kafka connector sources
            |  tests - tests, Docker based integration tests, code for the documentation
            |  testkit - framework for testing the connector
            |
            |Other modules:
            |  docs - the sources for generating https://doc.akka.io/docs/alpakka-kafka/current
            |  benchmarks - compare direct Kafka API usage with Alpakka Kafka
            |
            |Useful sbt tasks:
            |
            |  docs/previewSite
            |    builds Paradox and Scaladoc documentation, starts a webserver and
            |    opens a new browser window
            |
            |  verifyCodeStyle
            |    checks if all of the code is formatted according to the configuration
            |
            |  verifyDocs
            |    builds all of the docs
            |
            |  test
            |    runs all the tests
            |
            |  tests/it:test
            |    run integration tests backed by Docker containers
            |
            |  tests/testOnly -- -t "A consume-transform-produce cycle must complete in happy-path scenario"
            |    run a single test with an exact name (use -z for partial match)
            |
            |  benchmarks/it:testOnly *.AlpakkaKafkaPlainConsumer
            |    run a single benchmark backed by Docker containers
          """.stripMargin
    )
    .aggregate(core, testkit, tests, benchmarks, docs)

lazy val core = project
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(SitePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka"),
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        "com.typesafe.akka" %% "akka-discovery" % akkaVersion % Provided,
        "org.apache.kafka" % "kafka-clients" % kafkaVersion,
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.2"
      ) ++ silencer,
    Compile / compile / scalacOptions += "-P:silencer:globalFilters=[import scala.collection.compat._]",
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      ),
    mimaBinaryIssueFilters += ProblemFilters.exclude[Problem]("akka.kafka.internal.*")
  )

lazy val testkit = project
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .settings(commonSettings)
  .settings(
    name := "akka-stream-kafka-testkit",
    AutomaticModuleName.settings("akka.stream.alpakka.kafka.testkit"),
    JupiterKeys.junitJupiterVersion := "5.5.2",
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion,
        "org.testcontainers" % "kafka" % testcontainersVersion % Provided,
        "org.scalatest" %% "scalatest" % scalatestVersion % Provided,
        "junit" % "junit" % "4.12" % Provided,
        "org.junit.jupiter" % "junit-jupiter-api" % JupiterKeys.junitJupiterVersion.value % Provided,
        "org.apache.kafka" %% "kafka" % embeddedKafkaVersion % Provided exclude ("org.slf4j", "slf4j-log4j12"),
        "org.apache.commons" % "commons-compress" % "1.19" % Provided, // embedded Kafka pulls in Avro which pulls in commons-compress 1.8.1
        embeddedKafka % Provided exclude ("log4j", "log4j")
      ) ++ silencer,
    mimaPreviousArtifacts := Set(
        organization.value %% name.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))
      )
  )

lazy val tests = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .configs(IntegrationTest.extend(Test))
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(automateHeaderSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-tests",
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
        "io.confluent" % "kafka-avro-serializer" % confluentAvroSerializerVersion % Test
        excludeAll (ExclusionRule("log4j", "log4j"), ExclusionRule("org.slf4j", "slf4j-log4j12"),
        ExclusionRule("com.typesafe.scala-logging"), ExclusionRule("org.apache.kafka")),
        // See https://github.com/sbt/sbt/issues/3618#issuecomment-448951808
        "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar"),
        "org.testcontainers" % "kafka" % testcontainersVersion % Test,
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "io.spray" %% "spray-json" % "1.3.5" % Test,
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0" % Test, // ApacheV2
        "org.junit.vintage" % "junit-vintage-engine" % JupiterKeys.junitVintageVersion.value % Test,
        // See http://hamcrest.org/JavaHamcrest/distributables#upgrading-from-hamcrest-1x
        "org.hamcrest" % "hamcrest-library" % "2.2" % Test,
        "org.hamcrest" % "hamcrest" % "2.2" % Test,
        "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
        "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % Test,
        // Schema registry uses Glassfish which uses java.util.logging
        "org.slf4j" % "jul-to-slf4j" % slf4jVersion % Test,
        "org.mockito" % "mockito-core" % "2.24.5" % Test,
        embeddedKafka % Test exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12")
      ) ++ silencer ++ {
        scalaBinaryVersion.value match {
          case "2.13" =>
            Seq()
          case "2.12" | "2.11" =>
            Seq(
              "org.apache.kafka" %% "kafka" % kafkaVersion % Provided exclude ("org.slf4j", "slf4j-log4j12"),
              // sbt 1.3.x reports: Conflicting cross-version suffixes in: org.apache.kafka:kafka, com.typesafe.scala-logging:scala-logging
              "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % embeddedKafkaSchemaRegistry % Test
              excludeAll (ExclusionRule("log4j", "log4j"), ExclusionRule("org.slf4j", "slf4j-log4j12"),
              ExclusionRule("com.typesafe.scala-logging"), ExclusionRule("org.apache.kafka"))
            )
        }
      },
    resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
    publish / skip := true,
    whitesourceIgnore := true,
    Test / fork := true,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false,
    Test / unmanagedSources / excludeFilter := {
      HiddenFileFilter ||
      // TODO: Remove ignore once `"io.github.embeddedkafka" %% "embedded-kafka-schema-registry"` is
      // available for Kafka 2.4.0
      "SerializationTest.java" ||
      "SerializationSpec.scala" ||
      "EmbeddedKafkaWithSchemaRegistryTest.java"
    }
  )

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(/*BintrayPlugin,*/ MimaPlugin)
  .settings(commonSettings)
  .settings(
    name := "Alpakka Kafka",
    publish / skip := true,
    whitesourceIgnore := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/alpakka-kafka/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(
        ("\\.java\\.scala".r, _ => ".java")
      ),
    Paradox / siteSubdirName := s"docs/alpakka-kafka/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
        "embeddedKafka.version" -> embeddedKafkaVersion,
        "confluent.version" -> confluentAvroSerializerVersion,
        "scalatest.version" -> scalatestVersion,
        "scaladoc.akka.kafka.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
        "javadoc.akka.kafka.base_url" -> "",
        // Akka
        "akka.version" -> akkaVersion,
        "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/$AkkaBinaryVersion/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/$AkkaBinaryVersion/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/$AkkaBinaryVersion/",
        "javadoc.akka.link_style" -> "direct",
        "extref.akka-management.base_url" -> s"https://doc.akka.io/docs/akka-management/current/%s",
        // Kafka
        "kafka.version" -> kafkaVersion,
        "extref.kafka.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs%s",
        "javadoc.org.apache.kafka.base_url" -> s"https://kafka.apache.org/$kafkaVersionForDocs/javadoc/",
        "javadoc.org.apache.kafka.link_style" -> "frames",
        // Java
        "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
        "javadoc.base_url" -> "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/",
        "javadoc.link_style" -> "direct",
        // Scala
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
        "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
        // Testcontainers
        "testcontainers.version" -> testcontainersVersion,
        "javadoc.org.testcontainers.base_url" -> s"https://javadoc.jitpack.io/com/github/testcontainers/testcontainers-java/testcontainers/$testcontainersVersion/javadoc/",
        "javadoc.org.testcontainers.link_style" -> "frames"
      ),
    apidocRootPackage := "akka",
    paradoxRoots := List("index.html",
                         "release-notes/1.0-M1.html",
                         "release-notes/1.0-RC1.html",
                         "release-notes/1.0-RC2.html"),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifact := makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io"
  )

lazy val benchmarks = project
  .dependsOn(core, testkit)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(Defaults.itSettings)
  .settings(automateHeaderSettings(IntegrationTest))
  .settings(
    name := "akka-stream-kafka-benchmarks",
    publish / skip := true,
    whitesourceIgnore := true,
    IntegrationTest / parallelExecution := false,
    libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
        "io.dropwizard.metrics" % "metrics-core" % "3.2.6",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
        "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
        "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "1.1.2",
        "org.testcontainers" % "kafka" % testcontainersVersion % IntegrationTest,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % IntegrationTest,
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % IntegrationTest,
        "org.scalatest" %% "scalatest" % scalatestVersion % IntegrationTest
      )
  )
