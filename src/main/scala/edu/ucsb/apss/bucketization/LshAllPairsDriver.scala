package edu.ucsb.apss.bucketization

import edu.ucsb.apss.tokenization.TweetToVectorConverter
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


    def run(tweets:RDD[String], numBuckets:Int) = {
       val attempts = (0 to 20).toList.map(b => apss(tweets, numBuckets))
       val best =  attempts.reduce(
            (a, b) => {
                if (a._3 > b._3) a else b
            })
        (best._1, best._2)
    }




    def pullAnchors(tweets: RDD[SparseVector], numBuckets: Int): Array[SparseVector] = {
        tweets.sample(false, numBuckets).collect()
    }


    def createBucket(tweets: RDD[SparseVector], numBuckets: Int):SparseVectorBucketizer = {
        new SparseVectorBucketizer(tweets.takeSample(false, numBuckets))
    }

    def bucketizeTweet(vector:SparseVector, bucketizer:SparseVectorBucketizer): (String, SparseVector) = {
        (bucketizer.createBucketKey(vector), vector)
    }




    def apss(tweets:RDD[String], numBuckets:Int) = {
        val convertedTweets = tweets.map(converter.convertTweetToVector)
        val bucketizer =  createBucket(convertedTweets, numBuckets)
        val bucketizedTweets = convertedTweets.map(bucketizeTweet(_, bucketizer)).persist()
        val anchors = bucketizedTweets.keys.distinct().collect()
//        bucketizedTweets.foreach(println)
        val groupedTweets = anchors.map(a => bucketizedTweets.filter(_._1 equals a)).map(b => b.values)
        val bestOnes = groupedTweets.map(a => findBestPairForBucket(a, bucketizer)).flatMap(b => b)
        val best = bestOnes.reduce((a,b) => if(a._3>b._3) a else b)
        best
    }


    def findBestPairForBucket(input: RDD[SparseVector], bucketizer: SparseVectorBucketizer): Option[(SparseVector, SparseVector, Double)] = {
        val indexed = input.zipWithIndex()
        val allPairsNonReflexive = indexed.cartesian(indexed).filter(a => a._1._2 != a._2._2)
        if(allPairsNonReflexive.isEmpty())
            None
        else{
            val pairsWithCosSim = allPairsNonReflexive.map(a => {
                val (aVec, bVec) = (a._1._1, a._2._1)
                val cosSim = bucketizer.calculateCosineSimilarity(aVec, bVec)
                (aVec, bVec, cosSim)
            })

            val maxPair = pairsWithCosSim.reduce(
                (a, b) => {
                    if (a._3 > b._3) a else b
                })
            Some(maxPair)
        }

    }


}
