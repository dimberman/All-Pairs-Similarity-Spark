package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.{BucketAlias, NormalizedVector}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 12/10/15.
  */
class HoldensPartitioner extends Serializable {
    //    val l1Norm = new Normalizer(p = 1)
    //    val lInfNorm = new Normalizer(p = Double.PositiveInfinity)

    def l1Norm(v: SparseVector) = {
        v.values.map(math.abs).sum
    }


    def lInfNorm(v: SparseVector) = {
        v.values.map(math.abs).max
    }

    def sortByl1Norm(r: RDD[SparseVector]) = {
        r.map(a => (l1Norm(a), a)).sortByKey(true)
    }

    def sortBylinfNorm(r: RDD[SparseVector]) = {
        r.map(a => (lInfNorm(a), a)).sortByKey(true)
    }


    //TODO split these functions so that the normalization and partitioning are seperated
    def partitionByLinfNorm(r: RDD[SparseVector], numBuckets: Int): RDD[(Int, NormalizedVector)] = {
        val sorted = sortBylinfNorm(r).map(f => NormalizedVector(f._1, l1Norm(f._2), f._2))
        sorted.zipWithIndex().map(f => ((f._2 / numBuckets).toInt, f._1))
    }


    def partitionByL1Norm(r: RDD[SparseVector], numBuckets: Int, numVectors: Int): RDD[(Int, Iterable[NormalizedVector])] = {
        val sorted = sortByl1Norm(r).map(f => NormalizedVector(lInfNorm(f._2), f._1, f._2))
        sorted.zipWithIndex().map { case (vector, index) => ((index / (numVectors / numBuckets)).toInt, vector) }.groupByKey()
    }


    def determineBucketLeaders(r: RDD[(Int, Iterable[NormalizedVector])]): RDD[(Int, Double)] = {
        //        val normsOnly = r.map(f => (f._1, f._2.l1))
        //        TODO we might not need to do this
        //        normsOnly.reduceByKey(math.max)
        r.mapValues(b => b.last.l1)

    }


    def tieVectorsToHighestBuckets(inputVectors: RDD[(Int, Iterable[NormalizedVector])], leaders: Array[(Int, Double)], threshold:Double, sc: SparkContext): RDD[(Int, Iterable[NormalizedVector])] = {
        //this step should reduce the amount of data that needs to be shuffled
        val lInfNormsOnly = inputVectors.mapValues(_.map(_.lInf))
        //TODO would it be cheaper to pre-shuffle all the vectors into partitions and the mapPartition?
        val broadcastedLeaders = sc.broadcast(leaders)
        val buckets: RDD[Iterable[Int]] = lInfNormsOnly.map {
            case (bucket, norms) =>
                //TODO this is inefficient, can be done in O(logn) time, though it might not be important unless there are LOTS of buckets
                //TODO possibly use Collections.makeBinarySearch?
                val taperedBuckets = broadcastedLeaders.value.takeWhile(_._1 <= bucket).toList
                val b = Array[Int](0)
                norms.map(
                    n =>{
                        var current = 0
                        while ((threshold/n > taperedBuckets(current)._2) && current < taperedBuckets.size - 1)
                            current = current + 1
                        taperedBuckets(current)._1
                    }
                )


        }
        inputVectors.zip(buckets).map {
            case ((key, vec), matchedBuckets) =>
                vec.zip(matchedBuckets).foreach { case (v, leader) => v.associatedLeader = leader }
                (key, vec)
        }
    }


    def pullReleventValues(r: RDD[(Long, (Double, Double, Double, Double))]): RDD[(Long, BucketAlias)] = {
        val b = r.map(a => (a._1, BucketAlias(a._2))).reduceByKey((a, b) => {
            BucketAlias(math.max(a.maxLinf, b.maxLinf), math.min(a.minLinf, b.minLinf), math.max(a.maxL1, b.maxL1), math.min(a.minL1, b.minL1))
        })
        b

    }


}








