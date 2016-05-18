package edu.ucsb.apss.bucketization

import edu.ucsb.apss.lsh.SparseVectorBucketizer
import edu.ucsb.apss.preprocessing.TextToVectorConverter
import org.apache.spark.mllib.linalg.SparseVector

import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}

/**
  * Created by dimberman on 12/6/15.
  */
class SparseVectorBucketizerTest extends FlatSpec with Matchers with BeforeAndAfter {

  val dummySparse1 = new SparseVector(3, Array(1, 2, 3), Array(1, 1, 1))
  val dummySparse2 = new SparseVector(3, Array(4, 5, 6), Array(1, 1, 1))
  val dummySparse3 = new SparseVector(3, Array(1, 2, 3), Array(1, 1, 1))
  val dummySparse4 = new SparseVector(3, Array(1, 2, 4), Array(1, 1, 1))


  val inputList = Array(dummySparse1, dummySparse2,dummySparse3, dummySparse4)


  "The SparseVector Bucketizer" should "read in a number of sparse vectors and turn them into 'anchor points'" in {
    noException should be thrownBy {
      val test = new SparseVectorBucketizer(inputList)
    }
    val test = new SparseVectorBucketizer(inputList)
    test.anchors shouldEqual inputList
  }


  "cosineSimilarity" should "read in a sparse vector and determine the cosine similarity of that vector" in {
    val test = new SparseVectorBucketizer(inputList)
    val converter = new TextToVectorConverter
    test.calculateCosineSimilarity(dummySparse1, dummySparse2) shouldEqual 0.0
    test.calculateCosineSimilarity(dummySparse1, dummySparse3) should be  (1.0 +- .000001)
    test.calculateCosineSimilarity(dummySparse1, dummySparse4) should be  (.6666666 +- .000001 )

  }

  "createBucketKey" should "take in a vector and determine a key based on its cosine similarity to each anchor vector" in {
    val testSparse = new SparseVector(3, Array(1, 2, 3), Array(1, 1, 1))
    val test = new SparseVectorBucketizer(inputList)
    val s = test.createBucketKey(testSparse)
    s shouldEqual "1011"

  }





}
