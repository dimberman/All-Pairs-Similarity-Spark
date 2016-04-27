package edu.ucsb.apss.partitioning


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


