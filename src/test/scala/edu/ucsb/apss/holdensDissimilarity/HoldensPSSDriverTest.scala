package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.Context
import edu.ucsb.apss.partitioning.HoldensPartitioner
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
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
        val answer = driver.run(sc, vecs, 2, .5)
        val x = answer.collect().sortBy(_._1)
        x.foreach(println)

    }

    it should "not break when athere is a high threshold" in {
        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/100-tweets-bag.txt")
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 3, 0)
        val x = answer.collect()
        //        x.foreach(println)

    }


    it should "not break when there is a high threshold" in {
        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/10-tweets-bag.txt")
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 5, 0)
        val x = answer.collect()
//        x.foreach(println)

    }

    it should "a" in {
        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/1k-tweets-bag.txt")
        val converter = new TweetToVectorConverter
        val vecs =   par.map(converter.convertTweetToVector)
//        val v = vecs.collect()
//        val vec =vecs.first()

//        val normalizer = HoldensPartitioner.normalizer(vec)
//        for (i <- vec.values.indices) vec.values(i) = vec.values(i) / normalizer
//
//        var sum = 0.0
//        for (i <- vec.values.indices) sum += vec.values(i) * vec.values(i)
//

        val answer = driver.run(sc, vecs, 20, 0.8)
        val x = answer.collect()


        //        val answer = driver.run(sc, vecs, 1, 0)
//        val x = answer.collect()
//        println(sum)
    }



}
