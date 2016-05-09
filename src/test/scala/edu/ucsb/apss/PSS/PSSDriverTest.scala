package edu.ucsb.apss.PSS

import edu.ucsb.apss.{TestOutputGenerator, Context}
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by dimberman on 1/18/16.
  */
class PSSDriverTest extends FlatSpec with Matchers with BeforeAndAfter {
    import edu.ucsb.apss.util.PartitionUtil._

    val sc = Context.sc

    val driver = new PSSDriver


    "apss" should "calculate the most similar vectors" in {
        val par = sc.parallelize(Seq("a a a a", "a a b b", "a b f g ", "b b b b"))
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 1, .4)
        val x = answer.collect().sortBy(_._1)
        x.foreach(println)

    }

    it should "not break when athere is a high threshold" in {
        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/100-tweets-bag.txt")
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 5, .5)
        val x = answer.collect()
        //        x.foreach(println)

    }


    it should "not break when there is a high threshold" in {
        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/100-tweets-bag.txt")
        val converter = new TweetToVectorConverter
        val vecs = par.map(converter.convertTweetToVector)
        val answer = driver.run(sc, vecs, 3, 0.9)
        val x = answer.collect()
//        x.foreach(println)

    }



    it should "contian only correct output" in {
        val testData = TestOutputGenerator.run(sc, "/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/100-tweets-bag.txt")
        val e =  testData.mapValues{v => truncateAt(v,2)}.collect()
        val expected = e.toMap

        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/100-tweets-bag.txt")
        val vecs = par.map(BagOfWordToVectorConverter.convert)
//        val v = vecs.collect()
//          .map(_.toDense)
//        v.foreach(println)
        val answer = driver.run(sc, vecs, 5, 0.0).map{case(x,b,c) => ((x,b),c)}.mapValues(truncateAt(_,2)).collect()
        answer.foreach{
            case(i,j) =>
//                println(s"for pair $i, expected: ${expected(i)} got: $j")
                expected(i) shouldEqual (j +- .011)
        }

    }

    it should "a" in {
        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/1k-tweets-bag.txt")
        val converter = new TweetToVectorConverter
        val vecs =   par.map(converter.convertTweetToVector)
        val executionValues = List(.5,.7,.9)
        val buckets = 41
        val theoreticalStaticPartitioningValues = ArrayBuffer[Long]()
        val actualStaticPartitioningValues = ArrayBuffer[Long]()
        val dynamicPartitioningValues = ArrayBuffer[Long]()
        val timings = ArrayBuffer[Long]()



        for (i <- executionValues) {
            val threshold = i
            val t1 = System.currentTimeMillis()
            val answer = driver.run(sc, vecs, buckets, threshold).persist()

            val current = System.currentTimeMillis() - t1
            //            val top = answer.map { case (i, j, sim) => Sim(i, j, sim) }.top(10)
            //            println("breakdown: top 10 similarities")
            //            top.foreach(s => println(s"breakdown: $s"))
            theoreticalStaticPartitioningValues.append(driver.theoreticalStaticPairReduction)
            actualStaticPartitioningValues.append(driver.actualStaticPairReduction)
            dynamicPartitioningValues.append(driver.dParReduction)
            timings.append(current / 1000)
            answer.unpersist()
        }





        val numPairs = driver.numVectors * driver.numVectors / 2
        println("breakdown:")
        println("breakdown:")
        println("breakdown: ************histogram******************")
        //        println("breakdown:," + buckets.foldRight("")((a,b) => a + "," + b))
        println("breakdown:threshold," + executionValues.mkString(","))
        println("breakdown: theoretical pairs removed,"  + theoreticalStaticPartitioningValues.mkString(","))
        println("breakdown: actual pairs removed,"  + theoreticalStaticPartitioningValues.mkString(","))
        println("breakdown: theoretical % reduction,"  + theoreticalStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
        println("breakdown:actual % reduction," + actualStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
        println("breakdown:dynamic pairs filtered," + dynamicPartitioningValues.foldRight("")((a, b) => a + "," + b))
        println("breakdown:timing," + timings.mkString(","))
    }















//    it should "remove more pairs statically as the threshold goes up" in {
//        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/1k-tweets-bag.txt")
//        val converter = new TweetToVectorConverter
//        val vecs =   par.map(converter.convertTweetToVector)
//        val executionValues = List(0.5,0.7,0.8,0.9)
//        val staticPartitioningValues = ArrayBuffer[Long]()
//        val dynamicPartitioningValues = ArrayBuffer[Long]()
//        val timings = ArrayBuffer[Long]()
//
//
//        val driver = new PSSDriver
//
//        for (i <- executionValues) {
//            val threshold = i
//            val t1 = System.currentTimeMillis()
//            val answer = driver.run(sc, vecs, 21, threshold).persist()
//            answer.count()
//            val current = System.currentTimeMillis() - t1
//            val top = answer.map{case(i,j,sim) => Similarity(i,j,sim)}.top(10)
//            println("breakdown: top 10 similarities")
//            top.foreach(s => println(s"breakdown: $s"))
//            staticPartitioningValues.append(driver.sParReduction)
//            dynamicPartitioningValues.append(driver.dParReduction)
//            timings.append(current/1000)
//            answer.unpersist()
//        }
//
//        val numPairs = driver.numVectors*driver.numVectors/2
//        println("breakdown:")
//        println("breakdown:")
//        println("breakdown: ************histogram******************")
//        //        println("breakdown:," + buckets.foldRight("")((a,b) => a + "," + b))
//        println("breakdown:," + executionValues.foldRight("")((a,b) => a + "," + b))
//        println("breakdown:staticPairRemoval," + staticPartitioningValues.foldRight("")((a,b) => a + "," + b))
//        println("breakdown:static%reduction," + staticPartitioningValues.map(a => a.toDouble/numPairs).foldRight("")((a,b) => a + "," + b))
//        println("breakdown:dynamic," + dynamicPartitioningValues.foldRight("")((a,b) => a + "," + b))
//        println("breakdown:timing," + timings.foldRight("")((a,b) => a + "," + b))
//
//
//    }
//
//



}
