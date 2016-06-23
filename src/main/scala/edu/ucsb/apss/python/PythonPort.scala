package edu.ucsb.apss.python

import edu.ucsb.apss.PSS.{Similarity, PSSDriver}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, Row, Column, SQLContext}
import org.apache.spark.sql.types.StructType


/**
  * Created by dimberman on 6/22/16.
  *
  */

case class PySim(similarity:(Long, Long, Double))

object PythonPort {

    def cosine(df:DataFrame, sc:SparkContext, numBuckets:Int, threshold:Double) = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        val input = df.rdd.map( r => r.getAs[SparseVector]("vector"))
        val driver = new PSSDriver()
        driver.calculateCosineSimilarity(sc,input, numBuckets, threshold).map(x => PySim(x)).toDF()

    }
    def registerUdfs(sQLContext: SQLContext) = {
        sQLContext.udf.register("cosine-similarity",cosine _ )
    }

}
