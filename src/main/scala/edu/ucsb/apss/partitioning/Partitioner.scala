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


    def prepareTasksForParallelization[T](r: RDD[((Int, Int), T)], numBuckets: Int, neededVecs: Set[Int], needsSplitting: Map[(Int, Int), Long] = Map()): RDD[(Int, (Int, T))] = {
        //        val BVBucketValues = r.context.broadcast(bucketValues)
        val rNumBuckets = (numBuckets * (numBuckets + 1)) / 2
        val BVSums = r.context.broadcast(getSums(numBuckets))
        r.flatMap {
            case ((bucket, tiedLeader), v) =>
                val id = bucket * (bucket + 1) / 2 + tiedLeader
                val ga = assignPartition(rNumBuckets, id, BVSums.value)
                ga.filter { case (bId, buck) => isCandidate(buck, (bucket, tiedLeader)) && neededVecs.contains(bId) }
                  .flatMap {
                      case (ind, (buck, tv)) =>
                          val add = ind + rNumBuckets + 1
                          if (needsSplitting.contains((buck, tv)) && neededVecs.contains(add))
                              List((ind, (bucket, v)),(ind + rNumBuckets + 1, (bucket, v)))
                          else List((ind, (bucket, v)))
                  }
        }
    }



    def assignPartition(actualNum: Int, currentVal: Int, sums: Array[Int]): List[(Int, (Int, Int))] = {
        actualNum % 2 match {
            case 1 =>
                val e = List.range(currentVal + 1, (currentVal + 1) + (actualNum - 1) / 2) :+ currentVal
                val x = e.map(_ % actualNum)
                val v = pullTiedVectors(x, sums, x.head)
                e.zip(v)
            case 0 =>
                if (currentVal < actualNum / 2) {
                    val e = List.range(currentVal + 1, (currentVal + 1) + actualNum / 2).map(_ % actualNum) :+ currentVal
                    val v = pullTiedVectors(e, sums, e.head)
                    e.zip(v)
                }
                else {
                    val x = (currentVal + 1) + actualNum / 2 - 1
                    val e = List.range(currentVal + 1, x).map(_ % actualNum) :+ currentVal
                    val v = pullTiedVectors(e, sums, e.head)
                    e.zip(v)
                }
        }

    }

    def pullTiedVectors(list: List[Int], sums: Array[Int], startInd: Int): List[(Int, Int)] = {
        val a = list.scanLeft((0, 0)) {
            case ((bucket, tv), ind) =>
                var buck = bucket
                if (sums(buck) > ind) buck = 0
                while (ind >= sums(buck)) buck += 1
                val b =
                    if (ind != 0)
                        ind - sums(buck - 1)
                    else 0
                (buck, b)
        }
        a.tail

    }


    def isCandidate(a: (Int, Int), b: (Int, Int)): Boolean = {
        if (a._2 > b._1) false
        else true

    }

    def getSums(i: Int): Array[Int] = {
        val ret = new Array[Int](i + 1)
        ret(0) = 1
        for (j <- 1 to i) {
            ret(j) = ret(j - 1) + j + 1
        }
        ret
    }
}
