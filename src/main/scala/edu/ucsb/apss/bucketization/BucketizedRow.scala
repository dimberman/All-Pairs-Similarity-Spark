package edu.ucsb.apss.bucketization
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 1/10/16.
  */
case class BucketizedRow(summary:RowSummary, row:SparseVector)


object BucketizedRow{
    def apply(s:SparseVector, threshold:Double, index:Int):BucketizedRow = {
        val rowSum = s.values.sum
        val tm = threshold/s.values.max
        BucketizedRow(RowSummary(tm, rowSum, index, ""), s)
    }
}


case class RowSummary(tmax:Double, colSum:Double, index:Int,  bucket:String)
