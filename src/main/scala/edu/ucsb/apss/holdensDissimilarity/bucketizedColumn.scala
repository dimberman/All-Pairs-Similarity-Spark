package edu.ucsb.apss.holdensDissimilarity

/**
  * Created by dimberman on 1/6/16.
  */
case class BucketizedColumn(bucket:Int, colMax:Double, colSum:Double, index:Long)


case class ColumnSummary(tmax:Double, colSum:Double, index:Int)


object BucketizedColumn {
    implicit def orderingByIdAirportIdDelay[A <: BucketizedColumn] : Ordering[A] = {
        Ordering.by(fk => (fk.bucket, fk.colSum))
    }
}