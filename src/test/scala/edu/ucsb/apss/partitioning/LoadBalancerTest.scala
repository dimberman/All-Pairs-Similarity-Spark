package edu.ucsb.apss.partitioning

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.collection.mutable.{Set => MSet, Map => MMap, ListBuffer, ArrayBuffer}

/**
  * Created by dimberman on 4/26/16.
  */
class LoadBalancerTest extends FlatSpec with Matchers with BeforeAndAfter {
    type Key = (Int, Int)

    val testBucketSizes = Map(((1, 0), 10), ((2, 0), 30), ((3, 0), 41), ((4, 0), 40))
    val testLoad: Map[Key, List[Key]] = Map(
        ((1, 0), List()),
        ((2, 0), List()),
        ((3, 0), List((2,0))),
        ((4, 0), List((1,0),(2,0),(3,0)))
    )
    val testRefinementLoad:MMap[Key,MSet[Key]]= MMap() ++ Map(
        ((1, 0), MSet[Key]() ++ Set((4,0))),
        ((2, 0),  MSet[Key]() ++ Set((3,0),(4,0))),
        ((3, 0),  MSet[Key]() ++ Set((4,0))),
        ((4, 0),  MSet[Key]() ++ Set())
    )


    "initialLoadAssignment" should "redistribute in a greedy fashion" in {
        val testInput = MMap() ++ testLoad.mapValues(MSet() ++ _.toSet).map(identity)

        val answer = LoadBalancer.initialLoadAssignment(testInput, testBucketSizes)
        val expected = MMap() ++ Map(
            ((1, 0), MSet((4,0))),
            ((2, 0), MSet((3,0),(4,0))),
            ((3, 0), MSet((4,0))),
            ((4, 0), MSet())
        )
        println(answer.mkString("\n"))
        answer should contain allElementsOf expected
    }


    "loadRefinement" should "fix errors in the greedy algorithm" in {


        val answer = LoadBalancer.loadAssignmentRefinement(testRefinementLoad, testBucketSizes)
        val expected = MMap() ++ Map(
            ((1, 0), MSet((4,0))),
            ((2, 0), MSet((3,0))),
            ((3, 0), MSet((4,0))),
            ((4, 0), MSet((2,0)))
        )
        println(answer.mkString("\n"))
        answer should contain allElementsOf expected
    }
}
