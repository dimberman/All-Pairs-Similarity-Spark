package edu.ucsb.apss.PSS

import java.io.{BufferedReader, File}

import edu.ucsb.apss.util.FileSystemManager
import edu.ucsb.apss.{TestOutputGenerator, Context}
import edu.ucsb.apss.preprocessing.TextToVectorConverter
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * Created by dimberman on 1/18/16.
  */
class PSSDriverTest extends FlatSpec with Matchers with BeforeAndAfter {

    import edu.ucsb.apss.util.PartitionUtil._

    val sc = Context.sc

    val outputDir = s"/tmp/output/${sc.applicationId}/"
    val driver = new PSSDriver(local = true)

    after {
        val f = new File(outputDir)
        FileUtils.deleteDirectory(f)
    }

    "apss" should "calculate the most similar vectors" in {
        val par = sc.parallelize(Seq("a a a a", "a a b b", "a b f g ", "b b b b"))
        val converter = new TextToVectorConverter
        val vecs = par.map(converter.convertTextToVector(_))
        val answer = driver.calculateCosineSimilarity(sc, vecs, 1, .4, outputDirectory = outputDir + "a").collect()
        //        val x = answer.collect().sortBy(_._1)
        //        x.foreach(println)
        val expected = Array[(Long, Long, Double)]((1, 0, 0.7071067811865475),
            (2, 0, 0.5),
            (3, 1, 0.7071067811865475),
            (2, 1, 0.7071067811865475),
            (3, 2, 0.5))
        answer shouldEqual expected

    }

    it should "not break when athere is a high threshold" in {
        val par = sc.parallelize(Source.fromInputStream(getClass.getResourceAsStream("100-tweets-bag.txt")).getLines().toList)
        val converter = new TextToVectorConverter
        val vecs = par.map(converter.convertTextToVector(_))
        val answer = driver.calculateCosineSimilarity(sc, vecs, 5, .5, outputDirectory = outputDir + "1")
        val x = answer.collect()
        //        x.foreach(println)

    }

//
//    it should "not break when there is a high threshold" in {
//        val par = sc.textFile(getClass.getResource("100-tweets-bag.txt"))
//        val converter = new TextToVectorConverter
//        val vecs = par.map(converter.convertTextToVector(_))
//        val answer = driver.calculateCosineSimilarity(sc, vecs, 3, 0.9, outputDirectory = outputDir + "2")
//        val x = answer.collect()
//        //        x.foreach(println)
//
//    }

//    "asdlkfj" should "sdfalkj" in {
//        val outputDirec = s"${this.outputDir}15/correct/"
//        val d = new PSSDriver(local = true)
//
//        val testData = TestOutputGenerator.run(sc, Source.fromInputStream(getClass.getResource("100-tweets-bag.txt"))
//        val e = testData.mapValues { v => truncateAt(v, 2) }.collect()
//        val expected = e.toMap
//
//        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/100-tweets-bag.txt")
//        val vecs = par.map((new TextToVectorConverter).convertTextToVector(_))
//        //        val v = vecs.collect()
//        //          .map(_.toDense)
//        //        v.foreach(println)
//        val answer = d.calculateCosineSimilarity(sc, vecs, 5, 0.6, outputDirectory = outputDirec).collect().sorted
////         sc.textFile(outputDirec + "/*").map(s => s.split(",")).map(a => ((a(0).toLong, a(1).toLong), a(2).toDouble)).collect().sorted
//        println(s"count: ${answer.size}")
//        answer.foreach {
//            case (i, j,sim) =>
//                println(s"for pair $i, expected: ${expected((i,j))} got: $j")
//                expected((i,j)) shouldEqual (sim +- .011)
//        }
//    }

//    "qewerq" should "contian only correct output" in {
//        val outputDirec = s"${this.outputDir}18/correct"
//        val d = new PSSDriver(local = true)
//
//        val testData = TestOutputGenerator.run(sc, getClass.getResource("1k-tweets-bag.txt"))
//        val e = testData.mapValues { v => truncateAt(v, 2) }.collect()
//        val expected = e.toMap
//
//        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/1k-tweets-bag.txt")
//        val vecs = par.map((new TextToVectorConverter).convertTextToVector(_))
//
//        val answer = d.calculateCosineSimilarity(sc, vecs, 5, 0.6, outputDirectory = outputDirec).collect().sorted
//        println(s"count: ${answer.size}")
//        answer.foreach {
//            case (i, j,sim) =>
////                println(s"for pair $i, expected: ${expected((i,j))} got: $j")
//                expected((i,j)) shouldEqual (sim +- .011)
//        }
//
//    }
//
//
//
//
//
//
//    "the driver" should "a" in {
//        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/1k-tweets-bag.txt")
//
//        val converter = new TextToVectorConverter
//        val vecs = par.map(converter.convertTextToVector(_))
//        val executionValues = List(.9)
//        val buckets = 41
//        val theoreticalStaticPartitioningValues = ArrayBuffer[Long]()
//        val actualStaticPartitioningValues = ArrayBuffer[Long]()
//        val dynamicPartitioningValues = ArrayBuffer[Long]()
//        val timings = ArrayBuffer[Long]()
//
//
//
//        for (i <- executionValues) {
//            val threshold = i
//            val t1 = System.currentTimeMillis()
//            val answer = driver.calculateCosineSimilarity(sc, vecs, buckets, threshold, debug = true, outputDirectory = outputDir + "7").count()
//            val current = System.currentTimeMillis() - t1
//            theoreticalStaticPartitioningValues.append(driver.theoreticalStaticPairReduction)
//            actualStaticPartitioningValues.append(driver.actualStaticPairReduction)
//            dynamicPartitioningValues.append(driver.dParReduction)
//            timings.append(current / 1000)
//        }
//
//
//
//
//
//        val numPairs = driver.numVectors * driver.numVectors / 2
//        println("breakdown:")
//        println("breakdown:")
//        println("breakdown: ************histogram******************")
//        //        println("breakdown:," + buckets.foldRight("")((a,b) => a + "," + b))
//        println("breakdown:threshold," + executionValues.mkString(","))
//
//        println("breakdown: theoretical pairs removed," + theoreticalStaticPartitioningValues.mkString(","))
//        println("breakdown: actual pairs removed," + theoreticalStaticPartitioningValues.mkString(","))
//        println("breakdown: theoretical % reduction," + theoreticalStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
//        println("breakdown:actual % reduction," + actualStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
//        println("breakdown:dynamic pairs filtered," + dynamicPartitioningValues.foldRight("")((a, b) => a + "," + b))
//        println("breakdown:timing," + timings.mkString(","))
//    }
//
//
//
//    ignore should "b" in {
//        val par = sc.textFile("/Users/dimberman/Code/All-Pairs-Similarity-Spark/src/test/resources/edu/ucsb/apss/10k-clueweb.txt")
//        val converter = new TextToVectorConverter
//        val filter =converter.gatherCorpusWeightFilter(par, 100, false)
//        println("done creating filter")
//        val vecs = par.map(converter.convertTextToVector(_, maxWeight = 1000, removeSWords = true, topToRemove = 4, dfFilterSet = filter)).filter(_.values.nonEmpty)
//        val executionValues = List(.7)
//        val buckets = 21
//        val theoreticalStaticPartitioningValues = ArrayBuffer[Long]()
//        val actualStaticPartitioningValues = ArrayBuffer[Long]()
//        val dynamicPartitioningValues = ArrayBuffer[Long]()
//        val timings = ArrayBuffer[Long]()
//
//
//
//        for (i <- executionValues) {
//            val threshold = i
//            val t1 = System.currentTimeMillis()
//            val answer = driver.calculateCosineSimilarity(sc, vecs, buckets, threshold, outputDirectory = outputDir + "8").persist()
//
//            val current = System.currentTimeMillis() - t1
//            //            val top = answer.map { case (i, j, sim) => Sim(i, j, sim) }.top(10)
//            //            println("breakdown: top 10 similarities")
//            //            top.foreach(s => println(s"breakdown: $s"))
//            theoreticalStaticPartitioningValues.append(driver.theoreticalStaticPairReduction)
//            actualStaticPartitioningValues.append(driver.actualStaticPairReduction)
//            dynamicPartitioningValues.append(driver.dParReduction)
//            timings.append(current / 1000)
//            answer.unpersist()
//        }
//
//
//
//
//
//        val numPairs = driver.numVectors * driver.numVectors / 2
//        println("breakdown:")
//        println("breakdown:")
//        println("breakdown: ************histogram******************")
//        //        println("breakdown:," + buckets.foldRight("")((a,b) => a + "," + b))
//        println("breakdown:threshold," + executionValues.mkString(","))
//        println("breakdown: theoretical pairs removed," + theoreticalStaticPartitioningValues.mkString(","))
//        println("breakdown: actual pairs removed," + theoreticalStaticPartitioningValues.mkString(","))
//        println("breakdown: theoretical % reduction," + theoreticalStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
//        println("breakdown:actual % reduction," + actualStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
//        println("breakdown:dynamic pairs filtered," + dynamicPartitioningValues.foldRight("")((a, b) => a + "," + b))
//        println("breakdown:timing," + timings.mkString(","))
//    }

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
