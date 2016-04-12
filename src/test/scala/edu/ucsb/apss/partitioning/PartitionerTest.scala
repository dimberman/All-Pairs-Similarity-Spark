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
    val partitioner =  HoldensPartitioner

    "getSums" should "get the correct sum values to createPartitionAssignments can correctly tie values to buckets" in {
        val expected = Array(1,3, 6, 10)
        val answer =  partitioner.getSums(3)
        answer shouldEqual expected
    }



    "assignPartition" should "split fully with only two buckets" in {
        val sums = partitioner.getSums(2)
        val neededVecs = List((0,0),(1,0))
        partitioner.assignFixed(0, neededVecs, sums) shouldEqual List((0, 0),(1,0))
        partitioner.assignFixed(1, neededVecs, sums) shouldEqual List((1,0))

    }

    "assignPartition" should "split evenly with three buckets" in {
        val sums = partitioner.getSums(3)
        val neededVecs = List((0,0),(1,0), (2,0))
        partitioner.assignFixed(0, neededVecs, sums) shouldEqual List((0, 0),(1,0))
        partitioner.assignFixed(1, neededVecs, sums) shouldEqual List((1, 0),(2,0))
        partitioner.assignFixed(2, neededVecs, sums) shouldEqual List((0, 0),(2,0))


    }

    "assignPartition" should "evenly distribute partition assignments" in {
        val sums = partitioner.getSums(5)
        val neededVecs = List((0,0),(1,0),(2,0),(3,0),(4,0))
        partitioner.assignFixed(0, neededVecs, sums) shouldEqual List((0, 0),(1,0), (2,0))
    }

    it should "wrap around when it reaches the end" in {
        val sums = partitioner.getSums(5)
        val neededVecs = List((0,0),(1,0),(2,0),(3,0),(4,0))
        val fixed = partitioner.assignFixed(3, neededVecs, sums)
        fixed shouldEqual List((0, 0),(3,0), (4,0))
    }


    it should "uniquely compare each sub-bucket" in {
        val sums = partitioner.getSums(5)
        val neededVecs = List((0,0),(1,0),(2,0),(3,0),(4,0))
        val assignments = List(0,1,2,3,4) map {
            i =>
                partitioner.assignFixed(i, neededVecs, sums).map(List(_,(i,0)).sorted)
        }
        val allAssn = assignments.flatten
        val allVals = allAssn.map(a => (a.head, a.tail.head)).sorted
        allAssn.length shouldEqual allAssn.toSet.size
    }




    "prepareTasksForParallelization" should "take in an RDD with a tuple key and a bucketmapping and return a list " in {

    }
}
