package edu.ucsb.apss.bucketization

import edu.ucsb.apss.tokenization.TweetToVectorConverter
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 12/7/15.
  *
  * This class will handle phase 1 of the project: LSH based APSS
  * The essential idea is that we seperate each vector by a random group of "anchor vectors"
  * We then choose closest pairs within those buckets and then merge the buckets to find the "good enough" answer
  */
object LshAllPairsDriver {
  val converter = new TweetToVectorConverter

  def pullAnchors(tweets: RDD[SparseVector], numBuckets: Int): Array[SparseVector] = {
    tweets.sample(false, numBuckets).collect()

  }

  def bucketizeTweets(sc: SparkContext, tweets: RDD[String], numBuckets: Int):RDD[(String, SparseVector)] = {
    converter.generateTfWeights(tweets)
    val tweetVectors = tweets.map(converter.convertTweetToVector).map(_.toSparse).persist()
    val anchors = pullAnchors(tweetVectors, numBuckets)
    //TODO will this be expensive?
    val bucketizer = new SparseVectorBucketizer(anchors)
    tweetVectors.map(a => (bucketizer.createBucketKey(a), a))
  }

}
