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


    "createPartitioningAssignments" should "assure that each pair is matched exactly once" in {
        for(i <- 1 to 15){
//            println(s"processing $i")
            val bucketVals = sc.parallelize(List.range(0, i * (i + 1) / 2))
            val matchedPairs = bucketVals.cartesian(bucketVals)
              //getting rid of reflexive comparison
              //            .filter{case(x,c) => x!=c}
              //making order not matter (i.e. (1,3) == (3,1)
              .map{case(x,c) => if (x<c)(c,x) else (x,c)}.sortByKey()
              //getting rid of copies
              .distinct().collect().toList
            //          matchedPairs.foreach(println)
            val partitionLists = partitioner.createPartitioningAssignments(i).map(a => (a.taskBucket, a.values))
            val partitionPairs = partitionLists
              .flatMap{case (x, b) => b.map(c => (x,c))}
              .map{case(x,c) => if (x<c)(c,x) else (x,c)}
            partitionPairs.length shouldEqual matchedPairs.length
            //        partitionPairs.distinct.length shouldEqual matchedPairs.length
            partitionPairs should contain allElementsOf matchedPairs
        }

    }



    "prepareTasksForParallelization" should "take in an RDD with a tuple key and a bucketmapping and return a list " in {

    }
}
