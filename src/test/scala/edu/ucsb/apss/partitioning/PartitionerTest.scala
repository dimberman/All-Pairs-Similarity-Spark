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





    "prepareTasksForParallelization" should "take in an RDD with a tuple key and a bucketmapping and return a list " in {

    }
}
