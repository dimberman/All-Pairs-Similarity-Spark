package edu.ucsb.apss.tokenization

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 12/24/15.
  */
class BagOfWordToVectorConverterTest extends FlatSpec with Matchers with BeforeAndAfter {
    val converter = new BagOfWordToVectorConverter
    "The converter" should "read in a series of space delimited integers and convert them into a SparseVector" in {
        val input = "1 2 3 4 4"
        val convertedVector = converter.convert(input)
        convertedVector.indices shouldEqual Seq(1,2,3,4)
        convertedVector.values.sum should be (5.0 +- .000001)
        convertedVector.values.last should be (2.0 +- .00001)
    }
}
