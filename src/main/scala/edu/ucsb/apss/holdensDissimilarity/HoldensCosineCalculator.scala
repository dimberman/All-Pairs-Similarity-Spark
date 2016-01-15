package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.BucketMapping
import edu.ucsb.apss.bucketization.SparseVectorBucketizer
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 1/12/16.
  */
trait HoldensCosineCalculator {

    /**
      * This function will allow me to partition based on the BucketMappings and perform manual load balancing.
      * The problem I was running into before was that it was impossible to dynamically partition a value to multiple partitions.
      * To solve this, the following function flatmaps the values with a "PartitionKey" which can then be mapped via a repartition.
      * @param r
      * @param bucketValues
      * @return
      */
    def prepareTasksForParallelization[T](r:RDD[(Int, T)], bucketValues:List[BucketMapping]):RDD[(Int, (Int, T))] = {
        val BVBucketValues = r.context.broadcast(bucketValues)
        r.flatMap {
            case(bucket, v) =>
                BVBucketValues.value.flatMap(m => if (m.values.contains(bucket)) Some((m.taskBucket, (bucket, v))) else None)
        }
    }


    def createPartitioningAssignments(numBuckets: Int): List[BucketMapping] = {
        val masters: List[Int] = List.range(0, numBuckets)
        masters.map(
            m =>
                numBuckets % 2 match {
                    case 1 =>
                        val e = List.range(m + 1, (m + 1) + (numBuckets - 1) / 2)
                        val c = e.map(_ % numBuckets)
                        BucketMapping(m, c.toSet)
                    case 0 =>
                        if (m < numBuckets / 2)
                            BucketMapping(m, List.range(m + 1, (m + 1) + numBuckets / 2).map(_ % numBuckets).toSet)
                        else {
                            val x = (m + 1)  + numBuckets / 2 - 1
                            val e = List.range(m + 1, x)
                            val c = e.map(_ % numBuckets)
                            BucketMapping(m, c.toSet)
                        }
                }
        )
    }


    def writeInvertedIndexesToHDFS(r:RDD[(Int, SparseVector)]) = {

    }

}
