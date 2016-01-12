package edu.ucsb.apss

import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 1/2/16.
  */
case class VectorWithNorms(lInf: Double, l1: Double, vector: SparseVector, index:Int, var associatedLeader: Int = -1) extends Serializable

object VectorWithNorms {
    def apply(b: (Double, Double, SparseVector)): VectorWithNorms = {
        VectorWithNorms(b._1, b._2, b._3, -1)
    }
}




case class BucketAlias(maxLinf: Double, minLinf: Double, maxL1: Double, minL1: Double) extends Serializable {

}

object BucketAlias {
    def apply(a: (Double, Double, Double, Double)): BucketAlias = {
        BucketAlias(a._1, a._2, a._3, a._4)
    }
}
