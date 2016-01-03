package edu.ucsb.apss

import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.mllib.linalg.{Vectors, DenseVector, Vector}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.io.Source

/**
  * Created by dimberman on 12/24/15.
  */
class DIMSUMComparisonTest extends FlatSpec with Matchers with BeforeAndAfter {
    val sc = Context.sc

    "DIMSUM" should "return the sample tweets as pairs by similarity" in {
        //        val text = sc.textFile("src/test/resources/edu/ucsb/apss/sampleTweetVectors.txt")
        val text = sc.textFile("src/test/resources/edu/ucsb/apss/tinyTweetSample.txt")

        val vectors: RDD[Vector] = text.map((new BagOfWordToVectorConverter).convert)
        //        val vectors: RDD[Vector] = text.map(_.split(" ").map(_.toDouble)).map(new DenseVector(_))
        val d = text.collect()
        val b = vectors.collect()
        //        val mat = new RowMatrix(vectors, vectors.count(), 1048576)
        val mat = new RowMatrix(vectors)

        val a = mat.columnSimilarities(11.01)
        val exactEntries = a.entries.map { case MatrixEntry(i, j, u) => (u, (i, j)) }
//        exactEntries.sortByKey(false).foreach(println)


    }


    def transposeRowMatrix(m: RowMatrix): RowMatrix = {
        val transposedRowsRDD = m.rows.zipWithIndex.map { case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex) }
          .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
          .groupByKey
          .sortByKey().map(_._2) // sort rows and remove row indexes
          .map(buildRow) // restore order of elements in each row and remove column indexes
        new RowMatrix(transposedRowsRDD)
    }


    def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
        val indexedRow = row.toArray.zipWithIndex
        indexedRow.map { case (value, colIndex) => (colIndex.toLong, (rowIndex, value)) }
    }

    def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
        val resArr = new Array[Double](rowWithIndexes.size)
        rowWithIndexes.foreach { case (index, value) =>
            resArr(index.toInt) = value
        }
        Vectors.dense(resArr)
    }

}
