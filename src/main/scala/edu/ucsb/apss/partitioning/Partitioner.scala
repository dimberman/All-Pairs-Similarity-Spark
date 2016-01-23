package edu.ucsb.apss.partitioning

import edu.ucsb.apss.{VectorWithNorms, BucketMapping}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 1/12/16.
  */
trait Partitioner extends Serializable {

    /**
      * This function will allow me to partition based on the BucketMappings and perform manual load balancing.
      * The problem I was running into before was that it was impossible to dynamically partition a value to multiple partitions.
      * To solve this, the following function flatmaps the values with a "PartitionKey" which can then be mapped via a repartition.
      * @param r
      * @param bucketValues
      * @return
      */


    def prepareTasksForParallelization[T](r: RDD[((Int, Int), T)], numBuckets: Int): RDD[(Int, (Int, T))] = {
        //        val BVBucketValues = r.context.broadcast(bucketValues)
        val rNumBuckets = (numBuckets * (numBuckets + 1)) / 2
        val BVSums = r.context.broadcast(getSums(numBuckets))
        r.flatMap {
            case ((bucket, tiedLeader), v) =>
                val id = bucket * (bucket + 1) / 2 + tiedLeader
                //                BVBucketValues.value.flatMap(m => if (m.values.contains(id) ) Some((m.taskBucket, (bucket, v))) else None)
                assignPartition(rNumBuckets, id, BVSums.value).map(a => (a, (bucket, v)))
        }
    }


    def ltBinarySearch(a: Array[Int], key: Int): Int = {
        var low: Int = 0
        var high: Int = a.length - 1
        while (low <= high) {
            val mid: Int = (low + high) >>> 1
            val midVal: Int = a(mid)
            if (midVal < key) low = mid + 1
            else if (midVal > key) high = mid - 1
            else return mid
        }
        if (low == 0) 0
        else {
            val mid: Int = (low + high) >>> 1
            mid
        }

    }


    def assignPartition(actualNum: Int, currentVal: Int, sums: Array[Int]): List[Int] = {
        actualNum % 2 match {
            case 1 =>
                val e = List.range(currentVal + 1, (currentVal + 1) + (actualNum - 1) / 2) :+ currentVal
                val x = e.map(_ % actualNum)
                val y = ltBinarySearch(sums, currentVal)
                val v = pullTiedVectors(x, sums, y)
                x
            case 0 =>
                if (currentVal < actualNum / 2)
                    List.range(currentVal + 1, (currentVal + 1) + actualNum / 2).map(_ % actualNum) :+ currentVal
                else {
                    val x = (currentVal + 1) + actualNum / 2 - 1
                    val e = List.range(currentVal + 1, x)
                    val c = e.map(_ % actualNum) :+ currentVal
                    c
                }
        }

    }

    def pullTiedVectors(list: List[Int], sums: Array[Int], startInd: Int):List[(Int, Int)] = {
        list.scanRight((startInd, list.head - sums(startInd))){
            case (ind,(bucket, tv)) =>
                 val curSum = sums(bucket)
                 val next = (bucket + 1) % sums.length
                 val nextSum = sums(next)
                 if (ind == nextSum) {
                     (next, 0)
                 }
                 else {
                     (bucket, curSum - curSum)
                 }
        }

        //        var startSum = sums(startInd)
//        var i = startInd
//        val ret = new Array(list.size)
//        list.map(a => {
//            val curSum = sums(i)
//            val next = (i + 1) % sums.length
//            val nextSum = sums(next)
//            if(a ==nextSum) {
//                i = next
//                (next, 0)
//            }
//            else {
//                (i, a-curSum)
//            }
//        }
//        )
    }


    def isCandidate(a: (Int, Int), b: (Int, Int)): Boolean = {
        if (a._2 > b._1 && a._2 > b._2) false
        else true

    }



    def toAssignment(sums: Array[Int], input: Int) = {


    }

    def getSums(i: Int): Array[Int] = {
        val ret = new Array[Int](i + 1)
        ret(0) = 1
        for (j <- 1 to i) {
            ret(j) = ret(j - 1) + j + 1
        }
        ret
    }


    def writeInvertedIndexesToHDFS(r: RDD[(Int, SparseVector)]) = {

    }

}
