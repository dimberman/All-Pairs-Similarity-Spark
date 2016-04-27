package edu.ucsb.apss.partitioning

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 4/18/16.
  */
class PartitionHasherTest  extends FlatSpec with Matchers with BeforeAndAfter{
    val hasher = new PartitionHasher

    "the partition hasher" should "hash buckets into associated indexes"  in {
        val input = List((0,0),(1,1),(2,1),(7,0),(7,7))
        val expected = List(0,2,4,28,35)
        input.map(hasher.partitionHash) should contain allElementsOf expected
    }


    "the partition unhasher" should "hash numbers into associated buckets"  in {
        val input = List(0,2,4,28,35)
        val expected = List((0,0),(1,1),(2,1),(7,0),(7,7))

        input.map(hasher.partitionUnHash) should contain allElementsOf expected
    }

}
