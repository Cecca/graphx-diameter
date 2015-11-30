name := "graphx-diameter"

organization := "it.unipd.dei"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

spName := "Cecca/graphx-diameter"

sparkVersion := "1.4.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

spAppendScalaVersion := true

spIncludeMaven := false

sparkComponents := Seq("graphx")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test"
)

test in assembly := {}