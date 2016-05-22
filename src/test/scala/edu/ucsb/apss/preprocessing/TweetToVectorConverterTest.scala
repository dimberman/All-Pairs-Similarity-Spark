package edu.ucsb.apss.preprocessing

import edu.ucsb.apss.Context
import edu.ucsb.apss.util.PartitionUtil

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 12/7/15.
  */
class TweetToVectorConverterTest extends FlatSpec with Matchers with BeforeAndAfter{
      val sc = Context.sc

     "The Tweet Converter" should  "read in a tweet and convert it to the same vector every time"  in {
       val input = sc.parallelize(Seq("I like bananas", "bananas are yummy", "bananas", "bananas", "bananas"))
       val converter = new TweetToVectorConverter
       converter.generateTfWeights(input)
       val testData = sc.parallelize(Seq("I like bananas", "I like bananas", "bananas are yummy", "bananas", "bananas", "bananas"))
       val modifiedData = testData.map(converter.convertTweetToVector).collect()
       modifiedData(0) shouldEqual modifiedData(1)
       modifiedData(3) shouldEqual modifiedData(4)
     }

    it should  "normalize"  in {
        val input = "bananas yummy yummy bananas bananas candy candy apple charlie"
        val converter = new TweetToVectorConverter
        val vec = converter.convertTweetToVector(input)
        val normalized = PartitionUtil.normalizeVector(vec)
        println(normalized)
    }
}
