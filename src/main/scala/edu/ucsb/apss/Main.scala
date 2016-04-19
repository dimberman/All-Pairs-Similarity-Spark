package edu.ucsb.apss

import edu.ucsb.apss.PSS.PSSDriver
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


case class Sim(i:Long, j:Long, sim:Double) extends Ordered[Sim]{
    override def compare(that: Sim): Int = this.sim compare that.sim
    override def toString() = s"($i,$j): $sim"
}

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
        val executionValues = List(0.9)
        val vecs = par.map((new TweetToVectorConverter).convertTweetToVector)
        val staticPartitioningValues = ArrayBuffer[Long]()
        val dynamicPartitioningValues = ArrayBuffer[Long]()
        val timings = ArrayBuffer[Long]()


        val driver = new PSSDriver
        for (i <- executionValues) {
            val threshold = i
            val t1 = System.currentTimeMillis()
            val answer = driver.run(sc, vecs, 41, threshold).persist()
            answer.count()
            val current = System.currentTimeMillis() - t1
            log.info(s"breakdown: apss with threshold $threshold took ${current / 1000} seconds")
            val top = answer.map{case(i,j,sim) => Sim(i,j,sim)}.top(10)
            println("breakdown: top 10 similarities")
            top.foreach(s => println(s"breakdown: $s"))
            staticPartitioningValues.append(driver.sParReduction)
            dynamicPartitioningValues.append(driver.dParReduction)
            timings.append(current/1000)
            answer.unpersist()
        }

        val numPairs = driver.numVectors*driver.numVectors/2
        log.info("breakdown:")
        log.info("breakdown:")
        log.info("breakdown: ************histogram******************")
        log.info("breakdown:," + executionValues.foldRight("")((a,b) => a + "," + b))
        log.info("breakdown:staticPairRemoval," + staticPartitioningValues.foldRight("")((a,b) => a + "," + b))
        log.info("breakdown:static%reduction," + staticPartitioningValues.map(a => a.toDouble/numPairs).foldRight("")((a,b) => a + "," + b))
        log.info("breakdown:dynamic," + dynamicPartitioningValues.foldRight("")((a,b) => a + "," + b))
        log.info("breakdown:timing," + timings.foldRight("")((a,b) => a + "," + b))



    }

}
