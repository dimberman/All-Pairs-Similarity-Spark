package edu.ucsb.apss

import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 1/2/16.
  */
case class NormalizedVector(lInf: Double, l1: Double, vector: SparseVector, var associatedLeader: Int = -1) extends Serializable

object NormalizedVector {
    def apply(b: (Double, Double, SparseVector)): NormalizedVector = {
        NormalizedVector(b._1, b._2, b._3)
    }
}

case class BucketAlias(maxLinf: Double, minLinf: Double, maxL1: Double, minL1: Double) extends Serializable {

}

object BucketAlias {
    def apply(a: (Double, Double, Double, Double)): BucketAlias = {
        BucketAlias(a._1, a._2, a._3, a._4)
    }
}
