package edu.ucsb.apss.compartmentalize

import edu.ucsb.apss.util.VectorWithNorms
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dimberman on 4/15/16.
  */
class BucketCompartmentalizer(cSize: Int) extends Serializable {

    def aggregateBucket(input:RDD[((Int,Int), VectorWithNorms)]) = {
        input.aggregateByKey(ArrayBuffer[ArrayBuffer[VectorWithNorms]]())(addVector, mergeB).mapValues(a => a.map(_.toList).toList)
    }



    def addVector(a: ArrayBuffer[ArrayBuffer[VectorWithNorms]], vec: VectorWithNorms):ArrayBuffer[ArrayBuffer[VectorWithNorms]] = {
      var merged = false
        var i = 0
        while(i < a.size && !merged){
            if(a(i).size < cSize){
                merged = true
                a(i) += vec
            }
            i+=1
        }
        if(!merged){
            val n = ArrayBuffer[VectorWithNorms](vec)
            a += n
        }
        a
    }

    def mergeB(a: ArrayBuffer[ArrayBuffer[VectorWithNorms]], b: ArrayBuffer[ArrayBuffer[VectorWithNorms]]):ArrayBuffer[ArrayBuffer[VectorWithNorms]] = {
        a ++= b
        a
    }

}
