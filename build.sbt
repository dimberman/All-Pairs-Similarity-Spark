name := "MastersAllPairsSimilaritySearch"

version := "1.0"


scalaVersion := "2.10.5"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://raw.githubusercontent.com/ankurdave/maven-repo/master"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.0-M8"

libraryDependencies += "amplab" % "spark-indexedrdd" % "0.3"