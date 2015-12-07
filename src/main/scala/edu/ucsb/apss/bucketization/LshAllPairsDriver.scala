package edu.ucsb.apss.bucketization

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 12/7/15.
  *
  * This class will handle phase 1 of the project: LSH based APSS
  * The essential idea is that we seperate each vector by a random group of "anchor vectors"
  * We then choose closest pairs within those buckets and then merge the buckets to find the "good enough" answer
  */
object LshAllPairsDriver {
      def bucketizeTweets(sc:SparkContext, tweets:RDD[String]) = {

      }

}
