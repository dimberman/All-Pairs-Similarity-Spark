package edu.ucsb.apss

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dimberman on 12/7/15.
  */
object Context {
  val conf = new SparkConf().setMaster("local").setAppName("my app")
  conf.set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
   val sc = new SparkContext(conf)
    val sp = sc.accumulator(0)
}
