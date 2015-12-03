name := "graphx-diameter"

organization := "it.unipd.dei"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

spName := "Cecca/graphx-diameter"

sparkVersion := "1.4.1"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

sparkComponents := Seq("graphx")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test"
)

test in assembly := {}

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("https://github.com/Cecca/graphx-diameter"))

pomExtra := {
  <scm>
    <url>git@github.com:Cecca/graphx-diameter.git</url>
    <connection>scm:git:git@github.com:Cecca/graphx-diameter.git</connection>
  </scm>
    <developers>
      <developer>
        <id>Cecca</id>
        <name>Matteo Ceccarello</name>
        <url>https://github.com/Cecca</url>
      </developer>
    </developers>
}