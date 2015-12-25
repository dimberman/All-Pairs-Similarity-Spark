package edu.ucsb.apss.tokenization

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 12/24/15.
  *
  * This class is specifically meant for taking in the pre-processed Bag-of-word data for tweets and turning them into SparseVectors
  * for usage in Spark
  */
class BagOfWordToVectorConverter {
    def convert(s: String): SparseVector = {
        val hash = new HashingTF()
        val split = s.split(" ").map(_.toInt)

        val a = hash.transform(split).toSparse

        a
    }

}
