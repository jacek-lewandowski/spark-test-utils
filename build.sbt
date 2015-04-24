name := "spark-test"

version := "1.0"

scalaVersion := "2.10.5"

unmanagedBase <<= baseDirectory { base => new File(sys.env("SPARK_HOME")) / "lib" }

cleanKeepFiles ++= Seq("resolution-cache", "streams").map(target.value / _)
updateOptions := updateOptions.value.withCachedResolution(cachedResoluton = true)


