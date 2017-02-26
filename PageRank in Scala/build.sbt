
lazy val root = (project in file(".")).
    settings(
        name := "CalculatePageRank",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.0.1" )
    )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
