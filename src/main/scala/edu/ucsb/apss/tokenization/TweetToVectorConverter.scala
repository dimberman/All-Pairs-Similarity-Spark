package edu.ucsb.apss.tokenization

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 12/7/15.
  */
class TweetToVectorConverter extends Serializable{
  val table = new HashingTF

  def generateTfWeights(s:RDD[String]) = {
    table.transform(s.map(a => a.split(" ").toSeq))
  }

  def convertTweetToVector(s:String) = {
    table.transform(s.split(" ").toSeq)
  }

}
