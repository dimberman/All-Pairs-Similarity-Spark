package edu.ucsb.apss.lsh

import edu.ucsb.apss.Context
import edu.ucsb.apss.lsh.SparseVectorBucketizer
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by dimberman on 12/7/15.
  */
class LshAllPairsDriverTest extends FlatSpec with Matchers with BeforeAndAfter {

    val sc = Context.sc
    val anchors = Array("banana banana banana", "grape grape grape", "onion onion onion")
    val converter = new TweetToVectorConverter
    val bucketizer = new SparseVectorBucketizer(anchors.map(converter.convertTweetToVector))
    val driver =  LshAllPairsDriver

    val input = Seq("banana banana grape grape", "banana banana onion onion", "grape grape grape", "grape onion onion")
          .map(converter.convertTweetToVector)

    "bucketizeTweet" should "take in a tweet and return a key/value of the bucket to tweet" in {


    }


    it should "correctly map the tweets based on their cosine similarity to the anchors" in {
        driver.bucketizeTweet(input.head, bucketizer)._1 shouldEqual "110"
        driver.bucketizeTweet(input(1), bucketizer)._1 shouldEqual "101"
        driver.bucketizeTweet(input(2), bucketizer)._1 shouldEqual "010"
        driver.bucketizeTweet(input(3), bucketizer)._1 shouldEqual "001"
    }


    val answerVals =  Seq("a a a a", "a a a b").map(converter.convertTweetToVector)
    val testBucketValues = sc.parallelize(Seq("a a a a", "a a a b", "a a b b c", "b b b b").map(converter.convertTweetToVector))
    //TODO bucketizer should not have to do cosine similarity
    "findBestPairForBucket" should "take in a bucket and find the most similar pair within that bucket" in {
        val answer = driver.findBestPairForBucket(testBucketValues, bucketizer).get
        Seq(answer._1, answer._2) should contain allElementsOf answerVals

    }

    it should "not compare a vector to itself" in {
        val answer = driver.findBestPairForBucket(testBucketValues, bucketizer).get
        answer._1 equals  answer._2  shouldBe false

    }

//    it should "be able to take in a bucket which only contains one value without shutting down " in {
//
//        val b:RDD[String] = sc.parallelize(Seq("a","ab","abc"))
//
//
//        val c = b.filter(a => !a.contains("a"))
//        if(c.isEmpty()) println("")
//        else println(c.reduce(_+_))
//    }


    "edu/ucsb/apss" should "calculate the most similar vectors" in {
        val par =  sc.parallelize(Seq("a a a a a a a a a a", "a a b b", "a b f g ", "b b b b"))
        val answer = driver.runHoldens(par, 1)
        answer.entries.collect().foreach(println)

    }


}
