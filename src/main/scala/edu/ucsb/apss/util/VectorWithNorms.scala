package edu.ucsb.apss.util

import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 1/2/16.
  */

case class VectorWithNorms(lInf: Double, l1: Double, normalizer: Double, vector: SparseVector, index: Long, var associatedLeader: Int = -1) extends Serializable


object VectorWithNorms {
    def apply(b: (Double, Double,Double, SparseVector, Long)): VectorWithNorms = {
        VectorWithNorms(b._1, b._2,  b._3, b._4, b._5)
    }

    def apply(s:SparseVector):VectorWithNorms = {
        VectorWithNorms(0.0,0.0, 0.0,s,0,-1)
    }

    def apply(s:SparseVector, index:Long):VectorWithNorms = {
        VectorWithNorms(0.0,0.0, 0.0,s,index,-1)
    }
}




case class BucketAlias(maxLinf: Double, minLinf: Double, maxL1: Double, minL1: Double) extends Serializable {

}

object BucketAlias {
    def apply(a: (Double, Double, Double, Double)): BucketAlias = {
        BucketAlias(a._1, a._2, a._3, a._4)
    }
}
