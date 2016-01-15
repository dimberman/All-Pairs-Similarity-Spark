package edu.ucsb.apss.lsh

import org.apache.spark.mllib.linalg.Vector


/**
  * Created by dimberman on 12/6/15.
  */
abstract class VectorBucketizer extends Serializable{
  def calculateCosineSimilarity[T <: Vector](a:T, b:T): Double
}
