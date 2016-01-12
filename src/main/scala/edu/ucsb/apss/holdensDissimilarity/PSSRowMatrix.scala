package edu.ucsb.apss.holdensDissimilarity

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable.ListBuffer

/**
  * Created by dimberman on 1/6/16.
  */
class PSSRowMatrix(
                    rows: RDD[Vector],
                    private var nRows: Long,
                    private var nCols: Int) extends RowMatrix(rows, nRows, nCols) {


    def columnSimilarities(threshold: Double, numBuckets: Int): CoordinateMatrix = {


        val statistics = computeColumnSummaryStatistics()
        val (colMags, colMax, colSum) = (statistics.normL2, statistics.max, statistics.normL1)
        columnSimilaritiesPSS(colMags.toArray, colMax.toArray, colSum.toArray, threshold, numBuckets)
    }


    def columnSimilaritiesPSS(
                               colMags: Array[Double],
                               colMax: Array[Double],
                               colSum: Array[Double],
                               threshold: Double,
                               numBuckets: Int): CoordinateMatrix = {
        //        val bucketDivider: Int = (numBuckets / numCols()).toInt
        val summaries = rows.context.parallelize(colSum.zip(colMax)).zipWithIndex()
          .map { case ((sum, max), idx) =>
              require(idx<Int.MaxValue, "you are comparing too many values")
              (sum, (max, idx.toInt))
          }

        //TODO perhaps I should parallelize this one part and leave the rest local
        //TODO should only parallelize if dealing iwth a large N
        val sortedBySum = summaries.sortByKey().zipWithIndex().map { case ((sum, (max, idx)), idx2) => (idx2, ColumnSummary(threshold / max, sum, idx)) }.collectAsMap
        val BVsummaries = rows.context.broadcast(sortedBySum)


        val sims = rows.mapPartitionsWithIndex { (indx, iter) =>
            val colVal = BVsummaries.value
            val scaled = new Array[Double](colVal.size)
            iter.flatMap {
                row =>
                    row match {
                        case SparseVector(size, indices, values) =>
                            val nnz = indices.size
                            var k = 0
                            while (k < nnz) {
                                scaled(k) = values(colVal(k).index) / colVal(indices(colVal(k).index)).colSum
                                k += 1
                            }
                            Iterator.tabulate(nnz) { k =>
                                val buf = new ListBuffer[((Int, Int), Double)]()
                                val i = indices(k)
                                val iVal = scaled(k)
                                var l = colVal.size
                                while (colVal(l).colSum > colVal(k).tmax && l > k) {
                                    val j = indices(l)
                                    val jVal = scaled(l)
                                    buf += (((i, j), iVal * jVal))
                                    l += 1
                                }
                                buf
                            }.flatten
                        case DenseVector(values) =>
                            val n = values.size
                            var i = 0
                            while (i < n) {
                                scaled(i) = values(i) / colVal(i).colSum
                                i += 1
                            }
                            Iterator.tabulate(n) { i =>
                                val buf = new ListBuffer[((Int, Int), Double)]()
                                val iVal = scaled(i)
                                var j = n
                                while (colVal(j).colSum > colVal(i).tmax && j > i) {
                                    val jVal = scaled(j)
                                    buf += (((i, j), iVal * jVal))
                                    j += 1
                                }
                                buf
                            }.flatten
                    }
            }
        }

        val rSim = sims.reduceByKey(_ + _)
        val mSim = rSim.map { case ((i, j), sim) =>
            MatrixEntry(i.toLong, j.toLong, sim)
        }
        new CoordinateMatrix(mSim, numCols(), numCols())

    }





}
