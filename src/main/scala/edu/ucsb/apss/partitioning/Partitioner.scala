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





    def prepareTasksForParallelization[T](r: RDD[((Int, Int), T)], numBuckets: Int, neededVecs: List[(Int,Int)], needsSplitting: Map[(Int, Int), Long] = Map()): RDD[((Int,Int), T)] = {
        val numPartitions = (numBuckets * (numBuckets + 1)) / 2
        //TODO why is this 1 indexed?
        println("*************************")
        //Counts the sums of buckets : i.e. 2 would be 4 because 0,0 1,0 and 1,1 come before it (1 indexed)
        val BVSums = r.context.broadcast(getSums(numBuckets))

        val intermediate = r.flatMap {
            case ((bucket, tiedLeader), v) =>
                val vectorsToCompare = assignFixed(neededVecs.indexOf((bucket,tiedLeader)), neededVecs, BVSums.value)
                print(s"($bucket,$tiedLeader): ")
                vectorsToCompare.foreach(print)
                println()
                val filtered = vectorsToCompare.filter (
                    isCandidate(_, (bucket, tiedLeader))
                )

                filtered.map {
                    case buck =>

                        //                          if (needsSplitting.contains((buck, tv)) && neededVecs.contains(add))
                        //                              List((ind, (bucket, v)),(ind + numPartitions + 1, (bucket, v)))
                        (buck, v)
                    //                          List((ind, (bucket, v)))
                }
        }
        intermediate
    }



    def isCandidate(a: (Int, Int), b: (Int, Int)): Boolean = {
        //        if (a._2 > b._1 && a._1 > b._1) false
        //        else true

        true
    }

    def assignFixed(startingIndex:Int, neededVecs:List[(Int,Int)], sums:Array[Int]):List[(Int,Int)] = {
        val numberOfNeeded = neededVecs.length
        numberOfNeeded % 2 match {
            case 1 =>
                val proposedRange = List.range(startingIndex + 1, (startingIndex + 1) + (numberOfNeeded - 1) / 2) :+ startingIndex
                val modded = proposedRange.map(a => a%numberOfNeeded).toSet
                val pairs = neededVecs.zipWithIndex.filter(a => modded.contains(a._2)).map(_._1)
                pairs
            case 0 =>
                if (startingIndex < numberOfNeeded / 2) {
                    val e = (List.range(startingIndex + 1, (startingIndex + 1) + numberOfNeeded / 2).map(_ % numberOfNeeded) :+ startingIndex).toSet
                    val pairs = neededVecs.zipWithIndex.filter(a => e.contains(a._2)).map(_._1)
                    pairs
                }
                else {
                    val x = (startingIndex + 1) + numberOfNeeded / 2 - 1
                    val e = (List.range(startingIndex + 1, x).map(_ % numberOfNeeded) :+ startingIndex).toSet
                    val pairs = neededVecs.zipWithIndex.filter(a => e.contains(a._2)).map(_._1)
                    pairs
                }   
        }
        
    }
    
    



    def ltBinarySearch(a: List[Int], key: Int): Int = {
        var low: Int = 0
        var high: Int = a.length - 1
        while (low <= high) {
            val mid: Int = (low + high) >>> 1
            val midVal: Double = a(mid)
            if (midVal < key) low = mid + 1
            else if (midVal > key) high = mid - 1
            else return a(mid)
        }
        if(low == 0) 0
        else {
            val mid: Int = (low + high) >>> 1
            a(mid)
        }

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
