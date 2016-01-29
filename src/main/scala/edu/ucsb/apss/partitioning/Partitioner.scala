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
      * @param numBuckets
      * @return
      */


    def prepareTasksForParallelization[T](r: RDD[((Int, Int), T)], numBuckets: Int, neededVecs: Set[Int]): RDD[(Int, (Int, T))] = {
        //        val BVBucketValues = r.context.broadcast(bucketValues)
        val rNumBuckets = (numBuckets * (numBuckets + 1)) / 2
        val BVSums = r.context.broadcast(getSums(numBuckets))
        r.flatMap {
            case ((bucket, tiedLeader), v) =>
                val id = bucket * (bucket + 1) / 2 + tiedLeader
                val ga = assignPartition(rNumBuckets, id, BVSums.value)
                val x = 0
                ga.filter { case (bId, buck) => isCandidate(buck, (bucket, tiedLeader)) && neededVecs.contains(bId) }.map { case (ind, (buck, tv)) => (ind, (bucket, v)) }
        }
    }


    def gtBinarySearch(a: Array[Int], key: Int): Int = {
        var low: Int = 0
        var high: Int = a.length - 1
        var mid = 0
        while (low <= high) {
            mid = (low + high) >>> 1
            val midVal: Int = a(mid)
            if (midVal < key) low = mid + 1
            else if (midVal > key) high = mid - 1
            else if (mid == a.length - 1) return mid else return mid + 1
        }

        if (mid == a.length - 1) mid else mid + 1

    }


    def assignPartition(actualNum: Int, currentVal: Int, sums: Array[Int]): List[(Int, (Int, Int))] = {
        actualNum % 2 match {
            case 1 =>
                val e = List.range(currentVal + 1, (currentVal + 1) + (actualNum - 1) / 2) :+ currentVal
                val x = e.map(_ % actualNum)
                val y = gtBinarySearch(sums, currentVal)
                val v = pullTiedVectors(x, sums, x.head)
                e.zip(v)
            case 0 =>
                if (currentVal < actualNum / 2) {
                    val e = List.range(currentVal + 1, (currentVal + 1) + actualNum / 2).map(_ % actualNum) :+ currentVal
                    val y = gtBinarySearch(sums, currentVal)
                    val v = pullTiedVectors(e, sums, e.head)
                    e.zip(v)
                }
                else {
                    val x = (currentVal + 1) + actualNum / 2 - 1
                    val e = List.range(currentVal + 1, x).map(_ % actualNum) :+ currentVal
                    val y = gtBinarySearch(sums, currentVal)
                    val v = pullTiedVectors(e, sums, e.head)
                    e.zip(v)
                }
        }

    }

    def pullTiedVectors(list: List[Int], sums: Array[Int], startInd: Int): List[(Int, Int)] = {
        val x = 5
        val a = list.scanLeft((0, 0)) {
            case ((bucket, tv), ind) =>
                var buck = bucket
                if(sums(buck)>ind) buck = 0
                while (ind>=sums(buck)) buck+=1
                val c = tv
                val t = ind
                val curSum = sums(bucket)
                val next = (bucket + 1) % sums.length
                val nextSum = sums(next)

                val b =
                    if(ind != 0)
                        ind - sums(buck-1)
                    else 0
                (buck, b)

        }
        a.tail

    }


    def isCandidate(a: (Int, Int), b: (Int, Int)): Boolean = {
        if (a._2 > b._1) false
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
