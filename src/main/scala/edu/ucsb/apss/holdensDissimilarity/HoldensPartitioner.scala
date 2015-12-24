package edu.ucsb.apss.holdensDissimilarity

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
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


    //TODO split these functions so taht the normalization and partitioning are seperated
    def partitionByLinfNorm(r: RDD[SparseVector], numBuckets: Int): RDD[(Int, NormalizedVector)] = {
        val sorted = sortBylinfNorm(r).map(f => NormalizedVector(f._1, l1Norm(f._2), f._2))
        sorted.zipWithIndex().map(f => ((f._2 / numBuckets).toInt, f._1))
    }


    def partitionByL1Norm(r: RDD[SparseVector], numBuckets: Int, numVectors:Int): RDD[(Int, NormalizedVector)] = {
        val sorted = sortByl1Norm(r).map(f => NormalizedVector(lInfNorm(f._2), f._1, f._2))
        sorted.zipWithIndex().map(f => ((f._2/(numVectors/numBuckets)).toInt, f._1))
    }


    def determineBucketLeaders(r: RDD[(Int, NormalizedVector)]): RDD[(Int, Double)] = {
        val normsOnly = r.map(f => (f._1, f._2.l1))
        //TODO we might not need to do this
        normsOnly.reduceByKey(math.max)
    }


    def tieVectorsToHighestBuckets(inputVectors: RDD[(Int, NormalizedVector)], leaders: Array[(Int, Double)], sc: SparkContext): RDD[(Int, NormalizedVector)] = {
        //this step should reduce the ammount of data that needs to be shuffled
        val lInfNormsOnly = inputVectors.mapValues(_.lInf)
        //TODO would it be cheaper to pre-shuffle all the vectors into partitions and the mapPartition?
        val broadcastedLeaders = sc.broadcast(leaders)
        val buckets = lInfNormsOnly.map(
            norm => {
                //TODO this is inefficient, can be done in O(logn) time
                broadcastedLeaders.value.take(norm._1).takeWhile(_._1 < norm._1).lastOption match {
                    case None => norm._1
                    case Some(s) => s._1
                }
            }

        )
        inputVectors.zip(buckets).map(f = a => {
            val (key, vec) = a._1
            vec.associatedLeader = a._2
            (key, vec)
        })
    }


    def pullReleventValues(r: RDD[(Long, (Double, Double, Double, Double))]):RDD[(Long, BucketAlias)] = {
        val b = r.map(a => (a._1, BucketAlias(a._2))).reduceByKey((a, b) => {
            BucketAlias(math.max(a.maxLinf, b.maxLinf), math.min(a.minLinf, b.minLinf), math.max(a.maxL1, b.maxL1), math.min(a.minL1, b.minL1))
        })
        b

    }


}


case class NormalizedVector(lInf: Double, l1: Double, vector: SparseVector, var associatedLeader: Int = -1)

object NormalizedVector {
    def apply(b: (Double, Double, SparseVector)): NormalizedVector = {
        NormalizedVector(b._1, b._2, b._3)
    }
}

case class BucketAlias(maxLinf: Double, minLinf: Double, maxL1: Double, minL1: Double) {

}

object BucketAlias {
    def apply(a: (Double, Double, Double, Double)): BucketAlias = {
        BucketAlias(a._1, a._2, a._3, a._4)
    }
}
