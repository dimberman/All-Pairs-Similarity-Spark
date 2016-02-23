package edu.ucsb.apss.InvertedIndex

import edu.ucsb.apss.Context
import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import org.slf4j.Logger

import scala.util.Random
import scala.collection.mutable.{Map => MMap}


/**
  * Created by dimberman on 1/15/16.
  */
class InvertedIndexTest extends FlatSpec with Matchers with BeforeAndAfter {
    type IndexMap = MMap[Int, List[FeaturePair]]
    val sc = Context.sc
    val smallSet = List((8, List(FeaturePair(1, 0.15045161740652213))),
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

//    val multifeature =
//        List((7, List(FeaturePair(6, 0.07986027126441331),FeaturePair(9, 0.958882779544301),FeaturePair(0, 0.2659310233377631),FeaturePair(8, 0.10169873612295899),FeaturePair(0, 0.7484890847311721),FeaturePair(0, 0.45939544524918685),FeaturePair(7, 0.6764596115508948))),
//            (8, List(FeaturePair(7, 0.13963851959178186),FeaturePair(1, 0.32500409138156616),FeaturePair(7, 0.9450180111875928),FeaturePair(2, 0.863782346215628),FeaturePair(3, 0.8056673251901333),FeaturePair(8, 0.45386895456510234),FeaturePair(6, 0.12932264310123542))),
//            (4, List(FeaturePair(5, 0.465452152984831),FeaturePair(1, 0.014417972163658921),FeaturePair(1, 0.5296685342763423),FeaturePair(9, 0.47843310240321824),FeaturePair(2, 0.5374996374579513),FeaturePair(4, 0.8838970974640987),FeaturePair(6, 0.7383761532868337))),
//            (1, List(FeaturePair(2, 0.37199574650962397),FeaturePair(4, 0.92928646691877),FeaturePair(8, 0.26690352186200195),FeaturePair(1, 0.20053803219936506),FeaturePair(6, 0.2148943925302078),FeaturePair(0, 0.8365708439856373),FeaturePair(4, 0.30831400220418315))),
//            (5, List(FeaturePair(9, 0.9613003176076929),FeaturePair(5, 0.11268197480344266),FeaturePair(4, 0.20154371916856317),FeaturePair(7, 0.8590668601046808),FeaturePair(2, 0.4399867199964558),FeaturePair(5, 0.4991008157599206),FeaturePair(7, 0.13634275780760874))),
//            (1, List(FeaturePair(6, 0.8231567026377005),FeaturePair(9, 0.05798286566119959),FeaturePair(8, 0.623814063988079),FeaturePair(9, 0.9813216668208556),FeaturePair(0, 0.5026623990852574),FeaturePair(6, 0.44826631260035577),FeaturePair(1, 0.23215407972062863))),
//        )

    def smallFeaturePairSet = {
        print("List(")
        for (i <- 0 to 5) {
            generateFeaturePairs(5)
        }
        print(")")
    }


    def generateFeaturePairs(numFeatures: Int) = {
        val randBucket = Random
        val randWeight = Random
        var pair = ""
        for (i <- 0 to numFeatures) pair += s"FeaturePair(${math.abs(randBucket.nextInt()) % 10}, ${randWeight.nextDouble()}),"
        pair += s"FeaturePair(${math.abs(randBucket.nextInt()) % 10}, ${randWeight.nextDouble()})"
        println(s"(${math.abs(randBucket.nextInt()) % 10}, List($pair)),")
    }

    "gen" should "gen" in {
        smallFeaturePairSet
    }

    "deriveID" should "create unique IDs that should never collide" in {
        InvertedIndex.deriveID((10, 10),numParts = 0) == InvertedIndex.deriveID((11, 0),numParts = 0) shouldBe false
    }


    "mergeFeaturePairs" should "merge feature pairs when there is a collision" in {
        val v1 = MMap[Int, List[FeaturePair]]() + (1 -> List(FeaturePair(7, 0.778499535068532)))
        val v2 = MMap[Int, List[FeaturePair]]() + (1 -> List(FeaturePair(5, 0.778499535068532)))
        val answer = (MMap[Int, List[FeaturePair]]() + (1 -> List(FeaturePair(7, 0.778499535068532), FeaturePair(5, 0.778499535068532))), (1, 0))
        InvertedIndex.mergeFeaturePairs((v1, (1, 0)), (v2, (1, 0))) shouldEqual answer
    }



//    "generateInvertedIndex" should "split accordingly" in {
//        InvertedIndex.generateInvertedIndexes()
//    }
    //    "mergeInvertedIndexes" should "return an inverted index which contains all keys of both maps" in {
    //        val invertedIndex1 = InvertedIndex(smallSet)
    //        val invertedIndex2 = InvertedIndex(smallSet2)
    //        val merged = InvertedIndex.mergeInvertedIndexes(invertedIndex1, invertedIndex2).indices.keySet.toList
    //        merged should contain theSameElementsAs (smallSet ++ smallSet2).toMap.keySet
    //    }
    //
    //    it should "handle collisions by merging the list of featurePairs" in {
    //        val invertedIndex1 = InvertedIndex(smallSet)
    //        val invertedIndex2 = InvertedIndex(smallSet2)
    //        val merged = InvertedIndex.mergeInvertedIndexes(invertedIndex1, invertedIndex2).indices
    //        merged(9).length shouldEqual 2
    //        merged(9).map(_.id) should contain theSameElementsAs List(3,5)
    //    }


    //    "generatehistogram" should "calculate the most similar vectors" in {
    //        val par = sc.parallelize(Seq("a a a a", "a a b b", "a b f g ", "b b b b"))
    //        val converter = new TweetToVectorConverter
    //        val vecs = par.map(converter.convertTweetToVector)
    //        InvertedIndex.run(Array("Absdf", "3","4.0", "21532"), sc)
    //    }
}
