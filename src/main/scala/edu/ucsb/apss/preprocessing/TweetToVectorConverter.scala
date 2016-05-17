package edu.ucsb.apss.preprocessing

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
  * Created by dimberman on 12/7/15.
  */
class TweetToVectorConverter extends Serializable{

  def generateTfWeights(s:RDD[String]) = {
    val table = new HashingTF(10000)

    table.transform(s.map(a => a.split(" ").toSeq))
  }

  def convertTweetToVector(s:String):SparseVector = {
    val table = new HashingTF(10000)

    transform(s.split(" ").toSeq).toSparse
  }


  def indexOf(term: Any): Int = nonNegativeMod(term.##, 10000)

  /**
    * Transforms the input document into a sparse term frequency vector.
    */
  def transform(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    document.foreach { term =>
      val i = indexOf(term)
      val t =  termFrequencies.getOrElse(i, 0.0)
//      if (t > 2000){
//        val x = termFrequencies.filter(_._2 > 100)
//        println(term)
//      }
      termFrequencies.put(i, t + 1.0)
    }
    Vectors.sparse(10000, termFrequencies.toSeq)
  }


  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}
