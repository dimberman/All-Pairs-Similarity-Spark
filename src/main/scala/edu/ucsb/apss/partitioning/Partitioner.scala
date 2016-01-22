package edu.ucsb.apss.partitioning

import edu.ucsb.apss.{VectorWithNorms, BucketMapping}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 1/12/16.
  */
trait Partitioner {

    /**
      * This function will allow me to partition based on the BucketMappings and perform manual load balancing.
      * The problem I was running into before was that it was impossible to dynamically partition a value to multiple partitions.
      * To solve this, the following function flatmaps the values with a "PartitionKey" which can then be mapped via a repartition.
      * @param r
      * @param bucketValues
      * @return
      */
//    def prepareTasksForParallelization[T](r:RDD[((Int, Int), T)], bucketValues:List[BucketMapping]):RDD[(Int, (Int, T))] = {
////        val BVBucketValues = r.context.broadcast(bucketValues)
//        r.flatMap {
//            case((bucket, tiedLeader), v) =>
//                val id = bucket*(bucket + 1)/2 + tiedLeader
////                BVBucketValues.value.flatMap(m => if (m.values.contains(id) ) Some((m.taskBucket, (bucket, v))) else None)
//
//        }
//    }

    def prepareTasksForParallelization[T](r:RDD[((Int, Int), T)], numBuckets:Int):RDD[(Int, (Int, T))] = {
        //        val BVBucketValues = r.context.broadcast(bucketValues)
        r.flatMap {
            case((bucket, tiedLeader), v) =>
                val id = bucket*(bucket + 1)/2 + tiedLeader
            //                BVBucketValues.value.flatMap(m => if (m.values.contains(id) ) Some((m.taskBucket, (bucket, v))) else None)
                assignPartition((numBuckets*(numBuckets+1))/2,id).map(a => (a,(bucket, v)))
        }
    }

    def assignPartition(actualNum:Int, m:Int):List[Int] ={
        actualNum % 2 match {
            case 1 =>
                val e = List.range(m + 1, (m + 1) + (actualNum - 1) / 2) :+m
                e.map(_ % actualNum)
            case 0 =>
                if (m < actualNum / 2)
                    List.range(m + 1, (m + 1) + actualNum / 2).map(_ % actualNum) :+ m
                else {
                    val x = (m + 1)  + actualNum / 2 - 1
                    val e = List.range(m + 1, x)
                    val c = e.map(_ % actualNum):+m
                     c
                }
        }

    }




    def isCandidate(a:(Int, Int), b:(Int,Int)):Boolean = {
         if(a._2>b._1 && a._2 > b._2) false
        else true

    }


    def createPartitioningAssignments(numBuckets: Int): List[BucketMapping] = {
        val actualNum = (numBuckets*(numBuckets +1))/2
        val masters: List[Int] = List.range(0, actualNum)
        val sums = getSums(numBuckets)
        masters.par.map(
            m =>
                actualNum % 2 match {
                    case 1 =>
                        val e = List.range(m + 1, (m + 1) + (actualNum - 1) / 2) :+m
                        val c = e.map(_ % actualNum)
                        BucketMapping(m, c.toSet)
                    case 0 =>
                        if (m < actualNum / 2)
                            BucketMapping(m, (List.range(m + 1, (m + 1) + actualNum / 2).map(_ % actualNum) :+m ) .toSet  )
                        else {
                            val x = (m + 1)  + actualNum / 2 - 1
                            val e = List.range(m + 1, x)
                            val c = e.map(_ % actualNum):+m
                            BucketMapping(m, c.toSet)
                        }
                }
        ).toList
    }



    def toAssignment(sums:Array[Int], input:Int) = {


    }

    def getSums(i:Int):Array[Int] = {
        val ret = new Array[Int](i+1)
        ret(0)=1
        for(j <- 1 to i){
            ret(j) = ret(j-1) + j + 1
        }
        ret
    }




    def writeInvertedIndexesToHDFS(r:RDD[(Int, SparseVector)]) = {

    }

}
