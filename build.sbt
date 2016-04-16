name := "MastersAllPairsSimilaritySearch"

version := "1.0"

//lazy val commonSettings = Seq(
//    version := "0.1-SNasdgasdgAPSHOT",
//    organization := "edu.ucsb.apss",
//    scalaVersion := "2.10.5"
//)


scalaVersion := "2.10.4"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://raw.githubusercontent.com/ankurdave/maven-repo/master"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.14"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.0-M8"

libraryDependencies += "amplab" % "spark-indexedrdd" % "0.3"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.0.002"

//libraryDependencies += "com.typesafe.play" % "play_2.10" % "2.4.6"

assemblyJarName in assembly := s"apss-${version.value}.jar"

mainClass in assembly := Some("edu.ucsb.apss.Main")




assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala=false)