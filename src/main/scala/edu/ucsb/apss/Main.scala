package edu.ucsb.apss

import edu.ucsb.apss.holdensDissimilarity.HoldensPSSDriver
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger

import org.apache.spark.{Accumulator, SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

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
        println(s"taking in from ${args(0)}")
        println(s"default par: ${sc.defaultParallelism}")
        val executionValues = List(.6, .8, .9)
        val vecs = par.map((new TweetToVectorConverter).convertTweetToVector)
        val staticPartitioningValues = ArrayBuffer[Long]()
        val dynamicPartitioningValues = ArrayBuffer[Long]()
        val timings = ArrayBuffer[Long]()


        val driver = new HoldensPSSDriver
        for (i <- executionValues) {
            val threshold = i
            val t1 = System.currentTimeMillis()
            driver.run(sc, vecs, 30, threshold).count()
            val current = System.currentTimeMillis() - t1
            log.info(s"breakdown: apss with thresshold $threshold took ${current / 1000} seconds")
            staticPartitioningValues.append(driver.sParReduction)
            dynamicPartitioningValues.append(driver.dParReduction)
            timings.append(current/1000)
        }

        val numPairs = driver.numVectors*driver.numVectors/2
        log.info("breakdown:")
        log.info("breakdown:")
        log.info("breakdown: ************histogram******************")
        log.info("breakdown:," + executionValues.reduce((a,b)=> a + "," + b))
        log.info("breakdown:staticPairRemoval," + staticPartitioningValues.reduce((a,b) => a + "," + b))
        log.info("breakdown:static%reduction," + staticPartitioningValues.map(a => a.toDouble/numPairs).reduce((a,b) => a + "," + b))
        log.info("breakdown:dynamic," + dynamicPartitioningValues.reduce((a,b) => a + "," + b))


    }
}
