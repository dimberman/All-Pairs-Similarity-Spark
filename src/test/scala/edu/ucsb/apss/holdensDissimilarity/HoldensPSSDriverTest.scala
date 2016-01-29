package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.Context
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 1/18/16.
  */
class HoldensPSSDriverTest extends FlatSpec with Matchers with BeforeAndAfter {
    val sc = Context.sc

    val driver = new HoldensPSSDriver


    "apss" should "calculate the most similar vectors" in {
        val par = sc.parallelize(Seq("a a a a", "a a b b", "a b f g ", "b b b b"))
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 2, .01)
        val x = answer.collect().sortBy(_._1)
        x.foreach(println)

    }

    it should "not break when there is a high threshold" in {
        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/1k-tweets-bag.txt")
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 25, 2)
        val x = answer.collect()
//        x.foreach(println)

    }



}
