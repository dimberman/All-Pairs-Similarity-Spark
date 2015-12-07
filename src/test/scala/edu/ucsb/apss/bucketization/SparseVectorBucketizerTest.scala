package edu.ucsb.apss.bucketization

import org.apache.spark.mllib.linalg.SparseVector

import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}

/**
  * Created by dimberman on 12/6/15.
  */
class SparseVectorBucketizerTest extends FlatSpec with Matchers with BeforeAndAfter {

  val dummySparse1 = new SparseVector(3, Array(1, 2, 3), Array(.1, .2, .3))
  val dummySparse2 = new SparseVector(3, Array(4, 5, 6), Array(.1, .2, .3))
  val inputList = List(dummySparse1, dummySparse2)


  "The SparseVector Bucketizer" should "read in a number of sparse vectors and turn them into 'anchor points'" in {
    noException should be thrownBy {
      val test = new SparseVectorBucketizer(inputList)
    }
    val test = new SparseVectorBucketizer(inputList)
    test.anchors shouldEqual inputList
  }


  "cosineSimilarity" should "read in a sparse vector and determine the cosine similarity of that vector" in {

  }

  "createBucketKey" should "take in a vector and determine a key based on its cosine similarity to each anchor vector" in {

  }





}
