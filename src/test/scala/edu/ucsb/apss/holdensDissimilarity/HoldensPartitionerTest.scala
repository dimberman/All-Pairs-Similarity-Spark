package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.Context
import org.apache.spark.mllib.linalg.SparseVector
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 12/10/15.
  */
class HoldensPartitionerTest extends FlatSpec with Matchers with BeforeAndAfter {
    val partitioner = new HoldensPartitioner


    val sc = Context.sc
    val n = 4
    val indices = Array(0, 2, 3)
    val values = Array(Array(0.1, 0.3, 0.4), Array(0.1, 0.5, 0.4), Array(0.1, 0.9, 0.4), Array(0.1, 0.3, 1.4))
    val vectors = values.map(v => new SparseVector(n, indices, v)).toList

    val testRDD = sc.parallelize(Seq(
        new SparseVector(n, indices, Array(0.41 , 0.68 , 0.85)),
        new SparseVector(n, indices, Array(0.99 , 0.54 , 0.7)),
        new SparseVector(n, indices, Array(0.43 , 0.48 , 0.01)),
        new SparseVector(n, indices, Array(0.3 , 0.95 , 0.26)),
        new SparseVector(n, indices, Array(0.82 , 0.37 , 0.34)),
        new SparseVector(n, indices, Array(0.11 , 0.86 , 0.61)),
        new SparseVector(n, indices, Array(0.24 , 0.35 , 0.57)),
        new SparseVector(n, indices, Array(0.05 , 0.44 , 0.48)),
        new SparseVector(n, indices, Array(0.6 , 0.81 , 0.77)),
        new SparseVector(n, indices, Array(0.34 , 0.04 , 0.75)),
        new SparseVector(n, indices, Array(0.89 , 0.85 , 0.26)),
        new SparseVector(n, indices, Array(0.57 , 0.31 , 0.77)),
        new SparseVector(n, indices, Array(0.56 , 0.41 , 0.41)),
        new SparseVector(n, indices, Array(0.77 , 0.99 , 0.92)),
        new SparseVector(n, indices, Array(0.66 , 0.45 , 0.31)),
        new SparseVector(n, indices, Array(0.35 , 0.88 , 0.74)),
        new SparseVector(n, indices, Array(0.88 , 0.25 , 0.94)),
        new SparseVector(n, indices, Array(0.02 , 0.27 , 0.05)),
        new SparseVector(n, indices, Array(0.07 , 0.5 , 0.04)),
        new SparseVector(n, indices, Array(0.38 , 0.34 , 0.83))))


    def generateTestVectors(n:Int) = {
                for (i <- 0 to n) {
                    val a = truncateAt(math.random, 2)
                    val b =  truncateAt(math.random, 2)
                    val c =  truncateAt(math.random, 2)
                    println("new SparseVector(n, indices, Array(" + a + " , " +b + " , " + c + ")),")
                }
    }


    "mapVectorTol1Norm" should "take in a vector and create a keyvalue of that vector to it's l1norm" in {
        val norms = vectors.map(partitioner.l1Norm)
        val answer = List(0.8, 1.0, 1.4, 1.8)
        norms.zip(answer).foreach(n => n._1 should be(n._2 +- .000001))
    }

    "sortByLinfNorm" should "take in an RDD of SparseVectors and sort them by their LinfNorm" in {

    }

    "partitionByL1Norm" should "partition values into buckets blah blha blah" in {

    }

    "determineBucketLeaders" should "determine the max l1 value for a bucket and match it to the corresponding key" in {
        val bucketized = partitioner.partitionByL1Norm(sc.parallelize(vectors), 1, 4)
        val collected = bucketized.collect()
        partitioner.determineBucketLeaders(bucketized).collect().head._2 should be(1.8 +- .000001)

        val bucketizedLargeVec =  partitioner.partitionByL1Norm(testRDD, 4, 20)
        bucketizedLargeVec.keys.distinct().count() shouldEqual 4


    }

    "tieVectorsToHighestBuckets" should "take every vector and tie it to the bucket which has the closest but < leader to its lInf" in {
        val bucketizedVectors =  partitioner.partitionByL1Norm(testRDD, 4, 20)
        val leaders = partitioner.determineBucketLeaders(bucketizedVectors).collect()
        val tiedVectors = partitioner.tieVectorsToHighestBuckets(bucketizedVectors, leaders, sc)
        tiedVectors.foreach(a => println("bucket: " + a._1 + " is tied to leader " + a._2.associatedLeader)
        )

    }

    def truncateAt(n: Double, p: Int): Double = {
        val s = math pow (10, p); (math floor n * s) / s
    }

    it should "not go above the current bucket" in {

    }

    it should "handle the case where there are no buckets less than" in {

    }

    it should "set all associatedLeader values to a value other than -1"


}
