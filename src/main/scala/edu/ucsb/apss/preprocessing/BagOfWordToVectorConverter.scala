package edu.ucsb.apss.tokenization1

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 12/24/15.
  *
  * This class is specifically meant for taking in the pre-processed Bag-of-word data for tweets and turning them into SparseVectors
  * for usage in Spark
  */
object BagOfWordToVectorConverter extends Serializable{
    def convert(s: String) = {
        val hash = new HashingTF()
        val split = s.split(" ").map(i => i.toInt)

        val a = hash.transform(split).toSparse

        a
    }

    def revertToString(v:SparseVector):String = {
        v.indices.foldRight("")((i,s) => {
            val addOn = new StringBuilder
            val numAdd = v.values(i).toInt
            for(i <- 0 to numAdd) addOn.append(i + " ")
            s + addOn
        }
        )
    }

}
