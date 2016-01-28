package testPrep

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 1/28/16.
  */


class Transposer extends Serializable {
    def transpose(r:RDD[List[Int]]):RDD[List[Int]] = {
        val byColumnAndRow = r.zipWithIndex.flatMap {
            case (row, rowIndex) => row.zipWithIndex.map {
                case (number, columnIndex) => columnIndex -> (rowIndex, number)
            }
        }
        // Build up the transposed matrix. Group and sort by column index first.
        val byColumn = byColumnAndRow.groupByKey.sortByKey().values
        // Then sort by row index.
        val transposed = byColumn.map {
            indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
        }
        transposed.map(_.toList)

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
