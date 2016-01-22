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


    "edu/ucsb/apss" should "calculate the most similar vectors" in {
        val par = sc.parallelize(Seq("a a a a", "a a b b", "a b f g ", "b b b b"))
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 4, .01)
        val x = answer.collect()
//        x.foreach(a => a.foreach(println))
        x.foreach(println)


    }

}
