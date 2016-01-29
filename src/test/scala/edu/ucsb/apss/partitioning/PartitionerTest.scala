package edu.ucsb.apss.partitioning

import edu.ucsb.apss.{Context, BucketMapping}
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 1/21/16.
  */
class PartitionerTest extends FlatSpec with Matchers with BeforeAndAfter{
    val sc = Context.sc
    val testValue = sc.parallelize(Seq((1,1), "hello world"))
    val testMapping = new BucketMapping(1, Set(2,3))
    val partitioner = new HoldensPartitioner

    "getSums" should "get the correct sum values to createPartitionAssignments can correctly tie values to buckets" in {
        val expected = Array(1,3, 6, 10)
        val answer =  partitioner.getSums(3)
        answer shouldEqual expected
    }



    "pullTiedVectors" should "translate a partitionId into a bucket/TV pair" in {
        val input = List(2,3,4,5,6,8,15,0,1)
        val sums = partitioner.getSums(5)
        partitioner.pullTiedVectors(input, sums,1) shouldEqual List((1,1),(2,0),(2,1),(2,2), (3,0), (3,2), (5,0),(0,0),(1,0))
    }


    "assignPartition" should "evenly distribute partition assignments" in {
        val sums = partitioner.getSums(5)
        partitioner.assignPartition(5, 0, sums) shouldEqual List((1, (1, 0)), (2,(1,1)), (0,(0,0)))
    }



    "prepareTasksForParallelization" should "take in an RDD with a tuple key and a bucketmapping and return a list " in {

    }
}
