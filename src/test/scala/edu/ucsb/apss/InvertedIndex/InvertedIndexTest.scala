package edu.ucsb.apss.InvertedIndex

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.util.Random

/**
  * Created by dimberman on 1/15/16.
  */
class InvertedIndexTest extends FlatSpec with Matchers with BeforeAndAfter {


    val smallSet =List((8, List(FeaturePair(1, 0.15045161740652213))),
        (7, List(FeaturePair(6, 0.7607804088573142))),
        (9, List(FeaturePair(3, 0.9498220587970144))),
        (1, List(FeaturePair(9, 0.5191589251604092))),
        (4, List(FeaturePair(1, 0.3798288081401262))),
        (5, List(FeaturePair(1, 0.8138035245777492)))
    )

    val smallSet2 = List((3, List(FeaturePair(2, 0.6390544266043683))),
        (1, List(FeaturePair(7, 0.778499535068532))),
        (3, List(FeaturePair(8, 0.0248818711085953))),
        (1, List(FeaturePair(3, 0.030175073475522285))),
        (9, List(FeaturePair(5, 0.8773251380910161)))
    )
    def smallFeaturePairSet = {
        print("List(")
        for (i <- 0 to 5) {
            generateInvertedIndexes(1)
        }
        print(")")
    }




    def generateInvertedIndexes(numFeatures: Int) = {
        val randBucket = Random
        val randWeight = Random
        println(s"(${math.abs(randBucket.nextInt()) % 10}, List(FeaturePair(${math.abs(randBucket.nextInt()) % 10}, ${randWeight.nextDouble()}))),")
    }

    "mergeInvertedIndexes" should "return an inverted index which contains all keys of both maps" in {
        val invertedIndex1 = InvertedIndex(smallSet)
        val invertedIndex2 = InvertedIndex(smallSet2)
        val merged = InvertedIndex.mergeInvertedIndexes(invertedIndex1, invertedIndex2).indices.keySet.toList
        merged should contain theSameElementsAs (smallSet ++ smallSet2).toMap.keySet
    }

    it should "handle collisions by merging the list of featurePairs" in {
        val invertedIndex1 = InvertedIndex(smallSet)
        val invertedIndex2 = InvertedIndex(smallSet2)
        val merged = InvertedIndex.mergeInvertedIndexes(invertedIndex1, invertedIndex2).indices
        merged(9).length shouldEqual 2
        merged(9).map(_.id) should contain theSameElementsAs List(3,5)
    }
}
