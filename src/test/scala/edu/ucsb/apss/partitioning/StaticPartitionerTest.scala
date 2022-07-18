package edu.ucsb.apss.partitioning

import edu.ucsb.apss.Context
import edu.ucsb.apss.preprocessing.TextToVectorConverter
import org.apache.spark.mllib.linalg.SparseVector
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by dimberman on 12/10/15.
  */
class StaticPartitionerTest extends FlatSpec with Matchers with BeforeAndAfter {
    import edu.ucsb.apss.util.PartitionUtil._

    val partitioner = StaticPartitioner

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
        val norms = vectors.map(v => l1Norm(v))
        val answer = List(0.8, 1.0, 1.4, 1.8)
        norms.zip(answer).foreach(n => n._1 should be(n._2 +- .000001))
    }

    "sortByLinfNorm" should "take in an RDD of SparseVectors and sort them by their LinfNorm" in {

    }

    ignore should "partition values into buckets blah blha blah" in {
        val bucketizedLargeVec = partitioner.partitionByL1Sort(partitioner.recordIndex(testRDD), 4, 20)
        bucketizedLargeVec.keys.distinct().count() shouldEqual 4
        val bucketSizes = bucketizedLargeVec.mapValues(a => 1).reduceByKey(_+_).values.collect()
        bucketSizes.foreach(_ shouldBe 5)
    }


    ignore should "determine the max l1 value for a bucket and match it to the corresponding key" in {
        val bucketized = partitioner.partitionByL1Sort(partitioner.recordIndex(testRDD), 4, 20)
        val collected = bucketized.collect()
        val bucketLeaders = partitioner.determineBucketLeaders(bucketized.mapValues(extractUsefulInfo))
        val expected = Array((0, 1.13), (1, 1.53), (2, 1.97), (3, 2.68))
        bucketLeaders should contain allElementsOf expected
    }
//
    "tieVectorsToHighestBuckets" should "take every vector and tie it to the bucket which has the closest but < leader to its lInf" in {
        val rdd = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/10-tweets-bag.txt").map((new TextToVectorConverter).convertTextToVector(_))
        val normalized = partitioner.normalizeVectors(rdd)
        val bucketizedVectors = partitioner.partitionByL1Sort(partitioner.recordIndex(normalized), 4, normalized.count()).mapValues(extractUsefulInfo)
        bucketizedVectors.collect().foreach(a => println(s"bucket: ${a._1}, l1: ${a._2.l1} vec:${a._2}"))
        val sumLeaders = partitioner.determineBucketLeaders(bucketizedVectors).sortBy(a => a._1)
        val maxLeaders = partitioner.determineBucketMaxes(bucketizedVectors).sortBy(a => a._1)

        sumLeaders.foreach(a => println(s"leader ${a._1}: ${a._2}"))
        val threshold = .9
        val tiedVectors = partitioner.tieVectorsToHighestBuckets(bucketizedVectors, sumLeaders, maxLeaders, threshold, sc)
//        leaders.foreach{case (bucket, v) => println(s"leader for bucket $bucket: $v") }
        val collectedVectors = tiedVectors.collect()
        collectedVectors.foreach {
            case ((bucketIndex, tiedLeader), dr) =>
               val tmax =threshold/dr.lInf
//                if(tmax<sumLeaders(0)._2) {
//                    println(s"${threshold}/${dr.lInf} = $tmax, which is less than ${sumLeaders(0)._2}, therefore we place in G_($bucketIndex,$tiedLeader)")
//                    bucketIndex  shouldEqual tiedLeader
//                }
//                else {
//                    println(s"$threshold/${dr.lInf} = $tmax, which is greater than than ${sumLeaders(tiedLeader)._2}, therefore we place in G_($bucketIndex,$tiedLeader)")
//
////                    tmax should be > leaders(tiedLeader)._2
//                }
        }
    }



//
    it should "correctly handle the case where there is only one bucket" in {

    }

    it should "handle the case where the value is greater than the highest bucket" in {

    }



    it should "set all associatedLeader values to a value other than -1"  in {

    }











}

