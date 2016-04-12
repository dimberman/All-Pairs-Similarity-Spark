package edu.ucsb.apss

import edu.ucsb.apss.util.PartitionUtil.VectorWithNorms
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 1/2/16.
  */

object VectorWithNorms {
    def apply(b: (Double, Double,Double, SparseVector, Long)): VectorWithNorms = {
        VectorWithNorms(b._1, b._2,  b._3, b._4, b._5)
    }
}




case class BucketAlias(maxLinf: Double, minLinf: Double, maxL1: Double, minL1: Double) extends Serializable {

}

object BucketAlias {
    def apply(a: (Double, Double, Double, Double)): BucketAlias = {
        BucketAlias(a._1, a._2, a._3, a._4)
    }
}
