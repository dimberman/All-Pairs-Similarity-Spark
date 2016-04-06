package edu.ucsb.apss

import edu.ucsb.apss.holdensDissimilarity.HoldensPSSDriver
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger

import org.apache.spark.{Accumulator, SparkContext, SparkConf}

/**
  * Created by dimberman on 1/23/16.
  */
object Main {

    val log = Logger.getLogger(this.getClass)

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.yarn.executor.memoryOverhead", "600")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //        //TODO use kryoserializer
        //        //TODO MEMORY_ONLY_SER
        val sc = new SparkContext(conf)
        val par = sc.textFile(args(0))
        //        val idealNumExecutors = math.max(1,math.sqrt(sc.defaultParallelism * 4 ).toInt)
        //        val idealNumExecutors = args(1).toInt

        val skippedPairs: Accumulator[Int] = sc.accumulator(0)
        println(s"taking in from ${args(0)}")
        println(s"default par: ${sc.defaultParallelism}")
        //        println(s"numBuckets = $idealNumExecutors")
        val vecs = par.map((new TweetToVectorConverter).convertTweetToVector)
        val driver = new HoldensPSSDriver
                for(i <- 0 to 9){
                    val threshold = .1 * i
//                    val threshold = .9
                    val t1= System.currentTimeMillis()
                    driver.run(sc, vecs, 30, threshold).count()
                    val current = System.currentTimeMillis() - t1
                    log.info(s"breakdown: apss with thresshold $threshold took ${current/1000} seconds")
                }
//        for (i <- 0 to 8) {
//            val numBuckets = 20 + 5 * i
//            val t1 = System.currentTimeMillis()
//            driver.run(sc, vecs, numBuckets, .9).count()
//            val current = System.currentTimeMillis() - t1
//            log.info(s"breakdown: apss with $numBuckets buckets took ${current / 1000} seconds")
//        }


        //        answer.saveAsTextFile(args(1))
        //        for(arg <- args){
        //            log.info(arg)
        //        }

    }
}
