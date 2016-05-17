package edu.ucsb.apss.preprocessing

import edu.ucsb.apss.Context

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 12/7/15.
  */
class TextToVectorConverterTest extends FlatSpec with Matchers with BeforeAndAfter{
      val sc = Context.sc

     "The Tweet Converter" should  "read in a tweet and convert it to the same vector every time"  in {
       val input = sc.parallelize(Seq("I like bananas", "bananas are yummy", "bananas", "bananas", "bananas"))
       val converter = new TextToVectorConverter
       val testData = sc.parallelize(Seq("I like bananas", "I like bananas", "bananas are yummy", "bananas", "bananas", "bananas"))
       val modifiedData = testData.map(converter.convertTweetToVector(_)).collect()
       modifiedData(0) shouldEqual modifiedData(1)
       modifiedData(3) shouldEqual modifiedData(4)
     }
}
