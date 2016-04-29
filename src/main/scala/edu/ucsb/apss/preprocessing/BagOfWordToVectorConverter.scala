package edu.ucsb.apss.tokenization1

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.SparseVector
import scala.collection.mutable.{Map => MMap}


/**
  * Created by dimberman on 12/24/15.
  *
  * This class is specifically meant for taking in the pre-processed Bag-of-word data for tweets and turning them into SparseVectors
  * for usage in Spark
  */
object BagOfWordToVectorConverter extends Serializable{
    def convert(s: String):SparseVector = {


        if(s == ""){
            new SparseVector(0, Array(), Array())
        }
        else {
            val valMap:MMap[Int,Double] = MMap().withDefaultValue(0.0)
            for (i <- s.split(" ")){
                valMap(i.toInt) += 1
            }
            val ans = valMap.toArray



            new SparseVector(ans.length, ans.map(_._1), ans.map(_._2))
        }

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
