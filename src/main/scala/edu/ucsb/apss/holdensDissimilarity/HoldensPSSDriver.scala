package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 1/3/16.
  */
class HoldensPSSDriver {
    def run(sc:SparkContext, vectors:RDD[SparseVector], numBuckets:Int, threshold:Double) = {
        val count = vectors.count
        val partitioner = new HoldensPartitioner

        val partitionedVectors = partitioner.partitionByL1Norm(vectors, numBuckets, count).persist()
        val bucketLeaders = partitioner.determineBucketLeaders(partitionedVectors).collect()
        //TODO should I modify this so that it uses immutable objects?
        partitioner.tieVectorsToHighestBuckets(partitionedVectors, bucketLeaders, threshold, sc)

        val assignments = partitioner.createPartitioningAssignments(numBuckets)
        val tasks = partitioner.turnAssignmentsIntoRDDs(assignments, partitionedVectors)





    }
}
