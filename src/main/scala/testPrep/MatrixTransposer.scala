package testPrep

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 1/26/16.
  *
  * Since Spark's current solution only calculates similarity by column, This class will transpose the matrices to allow for equal testing.
  */
object MatrixTransposer {



    def main (args: Array[String]) {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.yarn.executor.memoryOverhead","600")
        val sc = new SparkContext(conf)
        val par = sc.textFile(args(0)).map(s => s.split(" ").map(_.toInt).toList)
        val t = new Transposer
        val res = t.transpose(par)
        res.saveAsTextFile(s"s3n://apss-masters/transposedTweets/results-${sc.applicationId}")
    }



}
