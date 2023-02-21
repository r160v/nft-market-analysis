
ThisBuild / version := "0.1.1"

ThisBuild / scalaVersion := "2.12.4"

lazy val root = (project in file("."))
  .settings(
    name := "nft-analysis"
  )

libraryDependencies ++= Seq(
  "net.openhft" % "zero-allocation-hashing" % "0.15",
  "net.openhft" % "chronicle-map" % "3.22ea5",
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0"
)

// SBT already finds jars present in the "lib" directory. However it is always best to express unmanaged dependencies
// explicitly. It eliminates scope of any assumptions and documents the dependencies right here in the "build.sbt" file.
Compile / unmanagedJars += baseDirectory.value / "lib/raphtory.jar"
