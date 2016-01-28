package edu.ucsb.apss

import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.mllib.linalg.{Vectors, DenseVector, Vector}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import testPrep.Transposer

import scala.io.Source

/**
  * Created by dimberman on 12/24/15.
  */
class DIMSUMComparisonTest extends FlatSpec with Matchers with BeforeAndAfter {
    val sc = Context.sc
//
//    "transpose" should "transpose a sparse matrix" in {
//        //        val text = sc.textFile("src/test/resources/edu/ucsb/apss/sampleTweetVectors.txt")
//        val text = sc.textFile("src/test/resources/edu/ucsb/apss/10k-tweets-bag.txt")
//
//        val vectors: RDD[Vector] = text.map(BagOfWordToVectorConverter.convert
//
//
//        )
//        //        val vectors: RDD[Vector] = text.map(_.split(" ").map(_.toDouble)).map(new DenseVector(_))
//        val d = text.collect()
////        d.foreach(println)
////        val b = vectors.collect()
////                val mat = new RowMatrix(vectors, vectors.count(), 1048576)
//        val mat = new RowMatrix(vectors)
//        mat.rows.collect().foreach(println)
////        val t = new Transposer
////      val trans = t.transposeRowMatrix(mat)
////        trans.rows.map(v => v.asInstanceOf[DenseVector].values.filter(_>0.0)).collect().foreach(a => {
////            a.foreach(c => print(s"$c "))
////            println
////        })
//
////        val a = mat.columnSimilarities(.1)
////        val exactEntries = a.entries.map { case MatrixEntry(i, j, u) => (u, (i, j)) }
////        exactEntries.sortByKey(false).foreach(println)
//
//
//    }




}
