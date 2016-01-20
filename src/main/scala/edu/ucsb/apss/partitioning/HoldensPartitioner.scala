package edu.ucsb.apss.partitioning

import edu.ucsb.apss.{BucketAlias, VectorWithNorms}
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import java.util

import scala.reflect.ClassTag

/**
  * Created by dimberman on 12/10/15.
  */
class HoldensPartitioner extends Serializable with Partitioner {
    //    val l1Norm = new Normalizer(p = 1)
    //    val lInfNorm = new Normalizer(p = Double.PositiveInfinity)

    def l1Norm(v: SparseVector) = {
        v.values.map(math.abs).sum
    }


    def lInfNorm(v: SparseVector) = {
        v.values.map(math.abs).max
    }

    def sortByl1Norm(r: RDD[(SparseVector, Long)]) = {
        r.map(a => (l1Norm(a._1), a)).sortByKey(true)
    }

    def partByl1Norm(r: RDD[(SparseVector, Long)], numBuckets: Int) = {
       val c =  r.map(a => (l1Norm(a._1), a))
         c.partitionBy(new RangePartitioner(numBuckets, c))
    }

    def partitionByL1Norm(r: RDD[SparseVector], numBuckets: Int, numVectors: Long): RDD[(Int, VectorWithNorms)] = {
        //        val a = r.collect()
        val sorted = sortByl1Norm(r.zipWithIndex()).map(f => VectorWithNorms(lInfNorm(f._2._1), l1Norm(f._2._1), f._2._1, f._2._2))
        sorted.zipWithIndex().map { case (vector, index) => ((index / (numVectors / numBuckets)).toInt, vector) }
    }


    def partitionByL1GraySort(r: RDD[SparseVector], numBuckets: Int, numVectors: Long): RDD[(Int, VectorWithNorms)] = {
        //        val a = r.collect()
        val sorted = partByl1Norm(r.zipWithIndex(), numBuckets).mapPartitionsWithIndex { case (idx, itr) =>
            itr.map {
                a =>
                    (idx, VectorWithNorms(lInfNorm(a._2._1), l1Norm(a._2._1), a._2._1, a._2._2))
            }

        }
//        sorted.zipWithIndex().map(a => (a._2.toInt, a._1))
        sorted
    }

    def determineBucketLeaders(r: RDD[(Int, VectorWithNorms)]): RDD[(Int, Double)] = {
        r.map{case(k,v) => (k, v.l1)}.reduceByKey(math.max)
    }


    var binarySearch: ((Array[Int], Int) => Int) =
        (l, x) => util.Arrays.binarySearch(l, x)


    def ltBinarySearch(a: List[(Int, Double)], key: Double): Int = {
        var low: Int = 0
        var high: Int = a.length - 1
        while (low <= high) {
            val mid: Int = (low + high) >>> 1
            val midVal: Double = a(mid)._2
            if (midVal < key) low = mid + 1
            else if (midVal > key) high = mid - 1
            else return a(mid)._1
        }
        val mid: Int = (low + high) >>> 1
        a(mid)._1
    }


    def tieVectorsToHighestBuckets(inputVectors: RDD[(Int, VectorWithNorms)], leaders: Array[(Int, Double)], threshold: Double, sc: SparkContext): RDD[(Int, VectorWithNorms)] = {
        //this step should reduce the amount of data that needs to be shuffled
        val lInfNormsOnly = inputVectors.mapValues(_.lInf)
        val broadcastedLeaders = sc.broadcast(leaders)
        val buckets: RDD[Int] = lInfNormsOnly.map {
            case (bucket, norms) =>
                val tmax = threshold / norms
                val taperedBuckets = broadcastedLeaders.value.take(bucket + 1).toList
                val current = ltBinarySearch(taperedBuckets, tmax)
                taperedBuckets(current)._1
        }
        inputVectors.zip(buckets).map {
            case ((key, vec), matchedBuckets) =>
                val nVec = new VectorWithNorms(vec.lInf, vec.l1, vec.vector, vec.index, matchedBuckets)
                (key, nVec)
        }
    }


    def filterPartition[T] = (b: Int, r: RDD[(Int, T)]) => r.filter { case (name, _) => name == b }


    def filterPartitionsWithDissimilarity = (a: Int, b: Int, r: RDD[(Int, VectorWithNorms)]) => r.filter { case (name, vec) => name == b && vec.associatedLeader >= a }


    def calculateSimilarities(r: RDD[((Int, VectorWithNorms), (Int, VectorWithNorms))]) = {

    }


    def pullReleventValues(r: RDD[(Long, (Double, Double, Double, Double))]): RDD[(Long, BucketAlias)] = {
        val b = r.map(a => (a._1, BucketAlias(a._2))).reduceByKey((a, b) => {
            BucketAlias(math.max(a.maxLinf, b.maxLinf), math.min(a.minLinf, b.minLinf), math.max(a.maxL1, b.maxL1), math.min(a.minL1, b.minL1))
        })
        b

    }


}








