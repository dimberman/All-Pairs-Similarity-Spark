package edu.ucsb.apss.preprocessing

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by dimberman on 12/7/15.
  */
class TextToVectorConverter extends Serializable{

  val stopWords = Set("the", "and")

  def convertTweetToVector(s:String, topToRemove:Int = 0, removeStopWords:Boolean = false, maxWeight:Int = Int.MaxValue):SparseVector = {
    val table =  generateTable(maxWeight)
    table.transform(s.split(" ").toSeq).toSparse
  }




  def removeStopWords(s:Seq[String]):Seq[String] = {
      s.filterNot(stopWords.contains)
  }

  def generateTable(maxWeight:Int) = {
    new HashingTF(10000){
      override def transform(document: Iterable[_]): Vector = {
        val termFrequencies = mutable.HashMap.empty[Int, Double]
        document.foreach { term =>
          val i = indexOf(term)
          val t= termFrequencies.getOrElse(i, 0.0)
          if (t < maxWeight)
            termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
        }
        Vectors.sparse(numFeatures, termFrequencies.toSeq)
      }
    }
  }

}
