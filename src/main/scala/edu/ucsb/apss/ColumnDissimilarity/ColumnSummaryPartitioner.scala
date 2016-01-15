package edu.ucsb.apss.ColumnDissimilarity

import org.apache.spark.Partitioner

/**
  * Created by dimberman on 1/6/16.
  */
class ColumnSummaryPartitioner(partitions:Int, buckets:Int, numCols:Long) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[BucketizedColumn]
        k.bucket % numPartitions
    }
}
