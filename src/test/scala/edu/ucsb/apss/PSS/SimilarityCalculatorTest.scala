package edu.ucsb.apss.PSS

import edu.ucsb.apss.InvertedIndex.{SimpleInvertedIndex, InvertedIndex}
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import edu.ucsb.apss.util.VectorWithNorms
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import scala.collection.mutable.{Map => MMap, ArrayBuffer}

/**
  * Created by dimberman on 4/28/16.
  */
class SimilarityCalculatorTest extends FlatSpec with Matchers with BeforeAndAfter {


    import edu.ucsb.apss.util.PartitionUtil._
    import edu.ucsb.apss.PSS.SimilarityCalculator._

    val aVectors = List("11 10 10 13 13 12 15 14 14 17 16 18 1 1 0 3 2 5 4 7 6 9 8 5", "42 24 25 26 27 20 21 21 22 23 28 29 40 40 41 39 38 38 19 32")
      .map(BagOfWordToVectorConverter.convert).map(normalizeVector)
    val bVectors = List("49 48 23 46 47 47 44 45 51 43 53 50 4 54 52", "59 58 19 57 56 60 61 62 8")
      .map(BagOfWordToVectorConverter.convert).map(normalizeVector)

    val aFeatures = aVectors.zipWithIndex
      .map { case (v, i) => VectorWithNorms(v, i) }.flatMap(InvertedIndex.createFeaturePairs)
    val bFeatures = bVectors.zipWithIndex
      .map { case (v, i) => VectorWithNorms(v, i) }.flatMap(InvertedIndex.createFeaturePairs)

    val invertedIndexes = List(InvertedIndex.addFeaturePairsToMap(MMap(), aFeatures.toArray),
        InvertedIndex.addFeaturePairsToMap(MMap(), bFeatures.toArray)).map(a => SimpleInvertedIndex(a.toMap))


    "calculateScores" should "" in {
        val score = Array.fill(2)(0.0)
        val indexMap = InvertedIndex.extractIndexMapFromSimple(invertedIndexes.head)
        val answer = ArrayBuffer[Double]()

        //        aVectors.foreach {
        //            v =>
        //                calculateScores(v,invertedIndexes.head.indices, indexMap, score)
        //                answer ++= score
        //                clearScoreArray(score)
        //
        //        }

        calculateScores(aVectors.head, invertedIndexes.head.indices, indexMap, score)
        answer ++= score
        println(s"index map => $indexMap")
        println(s"scores ${answer.mkString(",")}")

    }


    it should "a" in {
        val score = Array.fill(2)(0.0)
        val indexMap = InvertedIndex.extractIndexMapFromSimple(invertedIndexes.head)
        val answer = ArrayBuffer[Double]()

        aVectors.foreach {
            v =>
                calculateScores(v, invertedIndexes.head.indices, indexMap, score)
                answer ++= score
                clearScoreArray(score)

        }
        println(s"index map => $indexMap")
        println(s"scores ${answer.mkString(",")}")

    }


    it should "b" in {
        val score = Array.fill(2)(0.0)
        val indexMap = InvertedIndex.extractIndexMapFromSimple(invertedIndexes.head)
        val answer = ArrayBuffer[Double]()

        aVectors.foreach {
            v =>
                calculateScores(v, invertedIndexes.last.indices, indexMap, score)
                clearScoreArray(score)

        }

        aVectors.foreach {
            v =>
                calculateScores(v, invertedIndexes.head.indices, indexMap, score)
                answer ++= score
                clearScoreArray(score)

        }
        println(s"index map => $indexMap")
        println(s"scores ${answer.mkString(",")}")

    }



    "clear score array" should "remove all values from the score array and set all values to 0" in {
        val input = Array(1.0, 3.0, 4.0)
        clearScoreArray(input)
        input shouldEqual Array(0.0, 0.0, 0.0)
    }

}
