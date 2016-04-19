package edu.ucsb.apss.partitioning

import edu.ucsb.apss.util.PartitionUtil.VectorWithNorms
import edu.ucsb.apss.{VectorWithNorms, BucketMapping}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.json4s.jackson.Json

/**
  * Created by dimberman on 1/12/16.
  */
object PartitionHasher extends Serializable {
    def partitionHash(input: (Int, Int)) = {
        input._1 * (input._1 + 1) / 2 + 1 + input._2 - 1
    }


    def partitionUnHash(input: Int) = {
        var bucket = 0
        var index = 0
        var next = 0
        while (index + bucket <= input) {
            index = next
            bucket+= 1
            next += bucket
        }
        bucket = bucket - 1
        (bucket, input - index)
    }

}

class PartitionHasher extends Serializable{
    def partitionHash(input: (Int, Int)) = {
        input._1 * (input._1 + 1) / 2 + 1 + input._2 - 1
    }


    def partitionUnHash(input: Int) = {
        var bucket = 0
        var index = 0
        var next = 0
        while (index + bucket <= input) {
            index = next
            bucket+= 1
            next += bucket
        }
        bucket = bucket - 1
        (bucket, input - index)
    }
}


trait Partitioner extends Serializable {


    def partitionHash(input: (Int, Int)) = {
        input._1 * (input._1 + 1) / 2 + 1 + input._2
    }












    def ltBinarySearch(a: List[Int], key: Int): Int = {
        var low: Int = 0
        var high: Int = a.length - 1
        while (low <= high) {
            val mid: Int = (low + high) >>> 1
            val midVal: Double = a(mid)
            if (midVal < key) low = mid + 1
            else if (midVal > key) high = mid - 1
            else return a(mid)
        }
        if (low == 0) 0
        else {
            val mid: Int = (low + high) >>> 1
            a(mid)
        }

    }


    def getSums(i: Int): Array[Int] = {
        val ret = new Array[Int](i + 1)
        ret(0) = 1
        for (j <- 1 to i) {
            ret(j) = ret(j - 1) + j + 1
        }
        ret
    }
}
