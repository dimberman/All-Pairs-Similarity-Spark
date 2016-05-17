package edu.ucsb.apss.lsh

import edu.ucsb.apss.lsh.BucketizedRow
import edu.ucsb.apss.preprocessing.TextToVectorConverter
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by dimberman on 12/7/15.
  *
  * This class will handle phase 1 of the project: LSH based APSS
  * The essential idea is that we seperate each vector by a random group of "anchor vectors"
  * We then choose closest pairs within those buckets and then merge the buckets to find the "good enough" answer
  */
object LshAllPairsDriver {


    val converter = new TextToVectorConverter


    def run(tweets: RDD[String], numBuckets: Int) = {
        val attempts = (0 to 20).toList.map(b => apss(tweets, numBuckets))
        val best = attempts.reduce(
            (a, b) => {
                if (a._3 > b._3) a else b
            })
        (best._1, best._2)
    }


    def runHoldens(tweets: RDD[String], numBuckets: Int) = {
        val convertedTweets = tweets.map(converter.convertTweetToVector)
        val b = convertedTweets.collect()
        val bucketizer = createBucket(convertedTweets, numBuckets)
        val bucketizedTweets = convertedTweets.map(bucketizeTweet(_, bucketizer)).repartition(numBuckets).persist()
        bucketizedCosineSimilarity(bucketizedTweets.zipWithIndex().map{ case((bucket, vec), idx) => (vec, idx)}.map { case(vec, idx) => BucketizedRow(vec, .6, idx.toInt)})
    }


    def pullAnchors(tweets: RDD[SparseVector], numBuckets: Int): Array[SparseVector] = {
        tweets.sample(false, numBuckets).collect()
    }


    def createBucket(tweets: RDD[SparseVector], numBuckets: Int): SparseVectorBucketizer = {
        new SparseVectorBucketizer(tweets.takeSample(false, numBuckets))
    }

    def bucketizeTweet(vector: SparseVector, bucketizer: SparseVectorBucketizer): (String, SparseVector) = {
        (bucketizer.createBucketKey(vector), vector)
    }


    def apss(tweets: RDD[String], numBuckets: Int) = {
        val convertedTweets = tweets.map(converter.convertTweetToVector)
        val bucketizer = createBucket(convertedTweets, numBuckets)
        val bucketizedTweets = convertedTweets.map(bucketizeTweet(_, bucketizer)).persist()
        val anchors = bucketizedTweets.keys.distinct().collect()
        val groupedTweets = anchors.map(a => bucketizedTweets.filter(_._1 equals a)).map(b => b.values)
        val bestOnes = groupedTweets.map(a => findBestPairForBucket(a, bucketizer)).flatMap(b => b)
        val best = bestOnes.reduce((a, b) => if (a._3 > b._3) a else b)
        best
    }


    def findBestPairForBucket(input: RDD[SparseVector], bucketizer: SparseVectorBucketizer): Option[(SparseVector, SparseVector, Double)] = {
        val indexed = input.zipWithIndex()
        val allPairsNonReflexive = indexed.cartesian(indexed).filter(a => a._1._2 != a._2._2)
        if (allPairsNonReflexive.isEmpty())
            None
        else {
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

    //TODO create a bucketization flatmap that creates multiple copies of the same value with a "task" key (i.e. (1,3) would be (1,(1,3)), (3,(1,3)), (5,(1,3)) etc...


    def bucketizedCosineSimilarity(input: RDD[BucketizedRow]): CoordinateMatrix = {
        val n = input.count().toInt



        val sortedBytMax = input.map(_.summary).sortBy { case (tmax) => tmax.tmax }.collect()

        val sims = input.mapPartitions(
            iter => {
                var a = 0
                val b = iter.toStream
                b.flatMap(
                    summarizedRow => {
                        val row = summarizedRow.row
                        val buf = new ListBuffer[((Int, Int), Double)]()
                        b.drop(a+1).foreach(
                            summarizedRow2 => {
                                val row2 = summarizedRow2.row
                                if(summarizedRow.summary.tmax < summarizedRow2.summary.colSum){
                                    val c = ((summarizedRow.summary.index, summarizedRow2.summary.index), calculateCosineSimilarity(row, row2))
                                    buf += c
                                }
                            }
                        )
                        a+=1
                        buf
                    }
                ).toIterator
            }

        )

        val mSim = sims.map { case ((i, j), sim) =>
            MatrixEntry(i.toLong, j.toLong, sim)
        }
        new CoordinateMatrix(mSim, n, n)
    }


    def calculateCosineSimilarity(a: SparseVector, b: SparseVector): Double = {
        val a1Norm = magnitude(a.values)
        val b1Norm = magnitude(b.values)
        val norm = a1Norm * b1Norm
        val dot = dotProduct(a.indices.zip(a.values), b.indices.zip(b.values))
        dot / norm

    }

    private def magnitude(a: Array[Double]) = math.sqrt(a.foldLeft(0.0)((av, bv) => av + bv * bv))

    private def dotProduct(a: Array[(Int, Double)], b: Array[(Int, Double)]): Double = {
        val aMap = a.toMap
        val bMap = b.toMap
        (aMap.keySet ++ bMap.keySet)
          .foldLeft(List[Double]())(
              (b, i) => {
                  b :+ (aMap.getOrElse(i, 0.0) * bMap.getOrElse(i, 0.0))
              }
          ).sum
    }

}
