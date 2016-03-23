package edu.ucsb.apss.partitioning

import edu.ucsb.apss.{BucketAlias, VectorWithNorms}
import org.apache.spark.{Accumulator, RangePartitioner, SparkContext}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{HashMap => MMap}
import java.util


/**
  * Created by dimberman on 12/10/15.
  */
class HoldensPartitioner() extends Serializable with Partitioner {
    val skipped = MMap[Int,Int]().withDefault(a => 0)

    def l1Norm(v: SparseVector) = {
        v.values.map(math.abs).sum
    }


    def lInfNorm(v: SparseVector) = {
        v.values.map(math.abs).max
    }

    def normalizer(v: SparseVector) = {
        math.sqrt(v.values.map(a => a*a).sum)
    }


    def sortByl1Norm(r: RDD[(SparseVector, Long)]) = {
        r.map(a => (l1Norm(a._1), a)).sortByKey(true)
    }

    def partByl1Norm(r: RDD[(SparseVector, Long)], numBuckets: Int) = {
       val c =  r.map(a => (l1Norm(a._1), a))
         c.partitionBy(new RangePartitioner(numBuckets, c))
    }

    def partitionByL1Sort(r: RDD[SparseVector], numBuckets: Int, numVectors: Long): RDD[(Int, VectorWithNorms)] = {
        //        val a = r.collect()
        val sorted = sortByl1Norm(r.zipWithIndex()).map(f => VectorWithNorms(lInfNorm(f._2._1), l1Norm(f._2._1),normalizer(f._2._1), f._2._1, f._2._2))
        sorted.zipWithIndex().map { case (vector, index) => ((index / (numVectors / numBuckets)).toInt, vector) }
    }


    def partitionByL1GraySort(r: RDD[SparseVector], numBuckets: Int, numVectors: Long): RDD[(Int, VectorWithNorms)] = {
        val sorted = partByl1Norm(r.zipWithIndex(), numBuckets).mapPartitionsWithIndex { case (idx, itr) =>
            itr.map {
                case(l1, (vec, ind)) =>
                    (idx, VectorWithNorms(lInfNorm(vec), l1Norm(vec), normalizer(vec), vec, ind))
            }

        }
        sorted
    }

    def determineBucketLeaders(r: RDD[(Int, VectorWithNorms)]): RDD[(Int, Double)] = {
        r.map{case(k,v) => (k, v.l1)}.reduceByKey(math.max)
    }





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
        if(low == 0) 0
        else {
            val mid: Int = (low + high) >>> 1
            a(mid)._1
        }

    }


    def tieVectorsToHighestBuckets(inputVectors: RDD[(Int, VectorWithNorms)], leaders: Array[(Int, Double)], threshold: Double, sc: SparkContext): RDD[((Int,Int), VectorWithNorms)] = {
        //this step should reduce the amount of data that needs to be shuffled
        val persistedInputvecs = inputVectors.persist()
        val lInfNormsOnly = persistedInputvecs.mapValues(_.lInf)
        val broadcastedLeaders = sc.broadcast(leaders)
        val buckets: RDD[Int] = lInfNormsOnly.map {
            case (bucket, norms) =>
                val tmax = threshold / norms
                val taperedBuckets = broadcastedLeaders.value.take(bucket + 1).toList
                val current = ltBinarySearch(taperedBuckets, tmax)
                //TODO this is a hack
                val res = math.min(current, taperedBuckets.length - 1)
                taperedBuckets(res)._1
        }
        val ret = persistedInputvecs.zip(buckets).map {
            case ((key, vec), matchedBucket) =>
                //TODO mutable values would be faster
                val nVec = new VectorWithNorms(vec.lInf, vec.l1, vec.normalizer, vec.vector, vec.index, matchedBucket)
                ((key, matchedBucket), nVec)
        }
        persistedInputvecs.unpersist()
        ret
    }



    def filterPartitionsWithDissimilarity = (a: Int, b: Int, r: RDD[(Int, VectorWithNorms)]) => r.filter { case (name, vec) => name == b && vec.associatedLeader >= a }



    def pullReleventValues(r: RDD[(Long, (Double, Double, Double, Double))]): RDD[(Long, BucketAlias)] = {
        val b = r.map(a => (a._1, BucketAlias(a._2))).reduceByKey((a, b) => {
            BucketAlias(math.max(a.maxLinf, b.maxLinf), math.min(a.minLinf, b.minLinf), math.max(a.maxL1, b.maxL1), math.min(a.minL1, b.minL1))
        })
        b

    }


}








