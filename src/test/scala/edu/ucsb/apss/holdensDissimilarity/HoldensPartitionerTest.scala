package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.{VectorWithNorms, BucketMapping, Context}
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
        new SparseVector(n, indices, Array(0.41, 0.68, 0.85)),
        new SparseVector(n, indices, Array(0.99, 0.54, 0.7)),
        new SparseVector(n, indices, Array(0.43, 0.48, 0.01)),
        new SparseVector(n, indices, Array(0.3, 0.95, 0.26)),
        new SparseVector(n, indices, Array(0.82, 0.37, 0.34)),
        new SparseVector(n, indices, Array(0.11, 0.86, 0.61)),
        new SparseVector(n, indices, Array(0.24, 0.35, 0.57)),
        new SparseVector(n, indices, Array(0.05, 0.44, 0.48)),
        new SparseVector(n, indices, Array(0.6, 0.81, 0.77)),
        new SparseVector(n, indices, Array(0.34, 0.04, 0.75)),
        new SparseVector(n, indices, Array(0.89, 0.85, 0.26)),
        new SparseVector(n, indices, Array(0.57, 0.31, 0.77)),
        new SparseVector(n, indices, Array(0.56, 0.41, 0.41)),
        new SparseVector(n, indices, Array(0.77, 0.99, 0.92)),
        new SparseVector(n, indices, Array(0.66, 0.45, 0.31)),
        new SparseVector(n, indices, Array(0.35, 0.88, 0.74)),
        new SparseVector(n, indices, Array(0.88, 0.25, 0.94)),
        new SparseVector(n, indices, Array(0.02, 0.27, 0.05)),
        new SparseVector(n, indices, Array(0.07, 0.5, 0.04)),
        new SparseVector(n, indices, Array(0.38, 0.34, 0.83))))


    def generateTestVectors(n: Int) = {
        for (i <- 0 to n) {
            val a = truncateAt(math.random, 2)
            val b = truncateAt(math.random, 2)
            val c = truncateAt(math.random, 2)
            println("new SparseVector(n, indices, Array(" + a + " , " + b + " , " + c + ")),")
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
        val bucketizedLargeVec = partitioner.partitionByL1Norm(testRDD, 4, 20)
        bucketizedLargeVec.keys.distinct().count() shouldEqual 4
        val bucketSizes = bucketizedLargeVec.mapValues(a => 1).reduceByKey(_+_).values.collect()
        bucketSizes.foreach(_ shouldBe 5)
    }

    "determineBucketLeaders" should "determine the max l1 value for a bucket and match it to the corresponding key" in {
        val bucketized = partitioner.partitionByL1Norm(testRDD, 4, 20)
        val collected = bucketized.collect()
        val bucketLeaders = partitioner.determineBucketLeaders(bucketized).collect()
        val expected = Array((0, 1.13), (1, 1.53), (2, 1.97), (3, 2.68))
        bucketLeaders should contain allElementsOf expected
    }

    "tieVectorsToHighestBuckets" should "take every vector and tie it to the bucket which has the closest but < leader to its lInf" in {
        val bucketizedVectors = partitioner.partitionByL1Norm(testRDD, 4, 20)
        val leaders = partitioner.determineBucketLeaders(bucketizedVectors).collect().sortBy(a => a._1)
        val threshold = 1.5
        val tiedVectors = partitioner.tieVectorsToHighestBuckets(bucketizedVectors, leaders, threshold, sc)
        leaders.foreach{case (bucket, value) => println(s"leader for bucket $bucket: $value") }
        tiedVectors.foreach {
            case (bucketName, dr) =>
//                println(s"In bucket $bucketName ${dr.lInf} is tied to leader ${dr.associatedLeader} with a tmax ${threshold/dr.lInf}")
        }
    }

    def truncateAt(n: Double, p: Int): Double = {
        val s = math pow(10, p);
        (math floor n * s) / s
    }
//
    it should "not go above the current bucket" in {

    }

    it should "handle the case where there are no buckets less than" in {

    }

    it should "set all associatedLeader values to a value other than -1"  in {

    }

    "equallyPartitionTasksByKey" should "assure that each pair is matched exactly once" in {
      for(i <- 2 to 25){
          val bucketVals = sc.parallelize(List.range(0, i))
          val matchedPairs = bucketVals.cartesian(bucketVals)
            //getting rid of reflexive comparison
            .filter{case(x,c) => x!=c}
            //making order not matter (i.e. (1,3) == (3,1)
            .map{case(x,c) => if (x>c)(c,x) else (x,c)}.sortByKey()
            //getting rid of copies
            .distinct().collect().toList
//          matchedPairs.foreach(println)
          val partitionLists = partitioner.createPartitioningAssignments(i).map(a => (a.name, a.values))
          val partitionPairs = partitionLists
            .flatMap{case (x, b) => b.map(c => (x,c))}
            .map{case(x,c) => if (x>c)(c,x) else (x,c)}
          partitionPairs.length shouldEqual matchedPairs.length
          //        partitionPairs.distinct.length shouldEqual matchedPairs.length
          partitionPairs should contain allElementsOf matchedPairs
      }

    }


//    "turnAssignmentsIntoRDDs" should "only return the RDDs with keys linked to the bucketValues" in {
//        val n = VectorWithNorms(-1, -1, new SparseVector(4, indices, Array(0.41, 0.68, 0.85)))
//        val testNormalizedRDD = sc.parallelize(Seq((1,n),(2,n), (4,n), (5,n)))
//        val testBucketVal = List(BucketMapping(1, List(2,4)),BucketMapping(1, List(2,5)))
//        val assigned = partitioner.turnAssignmentsIntoRDDs(testBucketVal, testNormalizedRDD)
//        val results = assigned.map{case (name, v) => (name, v.collect().map(_._1))}
//        results.head._2 shouldEqual Array(2,4)
//    }


}

