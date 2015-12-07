package edu.ucsb.apss.bucketization

import org.apache.spark.mllib.linalg.{Vector, SparseVector}

/**
  * Created by dimberman on 12/6/15.
  *
  * This class should handle the generation of keys that will allow
  * us to bucketize the vector.
  */
class SparseVectorBucketizer(val anchors: List[SparseVector]) extends VectorBucketizer {
  val threshold = .5

    def calculateCosineSimilarity(a: SparseVector, b: SparseVector): Double = {
    val a1Norm = magnitude(a.values)
    val b1Norm = magnitude(b.values)
    val norm = a1Norm * b1Norm
    dotProduct(a.indices.zip(a.values), b.indices.zip(b.values)) / norm

  }

  private def magnitude(a: Array[Double]) = a.foldLeft(1.0)((av, bv) => av + bv * bv)

  private def dotProduct(a: Array[(Int, Double)], b: Array[(Int, Double)]): Double = {
    val aMap = a.toMap
    val bMap = b.toMap
    (aMap.keySet ++ bMap.keySet).map(i => aMap.getOrElse(i, 0.0) * bMap.getOrElse(i, 0.0)).sum
  }

  def createBucketKey(a:SparseVector):String = {
      anchors
        .map(calculateCosineSimilarity(a, _))
        .map(x => if (x>=threshold)1 else 0)
        .foldLeft("")((s,b) => s+b)
  }

  override def calculateCosineSimilarity[T <: Vector](a: T, b: T): Double = ???
}
