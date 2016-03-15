package edu.ucsb.apss.preprocessing

import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.mllib.linalg.SparseVector
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 12/24/15.
  */
class BagOfWordToVectorConverterTest extends FlatSpec with Matchers with BeforeAndAfter {
    val converter = BagOfWordToVectorConverter
    "The converter" should "read in a series of space delimited integers and convert them into a SparseVector" in {
        val input = "1 2 3 4"
        val convertedVector = converter.convert(input)
        val expected = new SparseVector(1048576, Array(1,3), Array(2.0,4.0))
        convertedVector shouldEqual expected
    }

    it should "handle cases where there is an empty string" in {
        val input = ""
        val convertedVector = converter.convert(input)
        val expected = new SparseVector(1048576, Array(), Array())
        convertedVector shouldEqual expected
    }
}
