package edu.ucsb.apss

import edu.ucsb.apss.holdensDissimilarity.HoldensPSSDriver
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dimberman on 1/23/16.
  */
object Main {

    val log = Logger.getLogger(this.getClass)
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.yarn.executor.memoryOverhead","600")
//          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //        //TODO use kryoserializer
//        //TODO MEMORY_ONLY_SER
        val sc = new SparkContext(conf)
        val par = sc.textFile(args(0))
        //        val idealNumExecutors = math.max(1,math.sqrt(sc.defaultParallelism * 4 ).toInt)
//        val idealNumExecutors = args(1).toInt

        println(s"taking in from ${args(0)}")
        println(s"default par: ${sc.defaultParallelism}")
//        println(s"numBuckets = $idealNumExecutors")
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val driver = new HoldensPSSDriver
        val answer = driver.run(sc, vecs, 30, 20)
        val x = answer.saveAsTextFile(s"s3n://apss-masters/answer-${sc.applicationId}.txt")
//        for(arg <- args){
//            log.info(arg)
//        }
    }
}
