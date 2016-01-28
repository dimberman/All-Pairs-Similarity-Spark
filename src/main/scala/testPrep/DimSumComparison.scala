package testPrep

import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dimberman on 1/28/16.
  */
object DimSumComparison {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.yarn.executor.memoryOverhead","600")
        val sc = new SparkContext(conf)
        val text = sc.textFile(args(0))

        val vectors: RDD[Vector] = text.map(BagOfWordToVectorConverter.convert)
        //        //        val vectors: RDD[Vector] = text.map(_.split(" ").map(_.toDouble)).map(new DenseVector(_))
        //        val d = text.collect()
        //        val b = vectors.collect()
//        val mat = new RowMatrix(vectors, vectors.count(), 1048576)
        val mat = new RowMatrix(vectors)

        val a = mat.columnSimilarities(.1)
        //        val exactEntries = a.entries.map { case MatrixEntry(i, j, u) => (u, (i, j)) }
        a.entries.saveAsTextFile(s"s3n://apss-masters/dimsumResults/results-${sc.applicationId}")
        //
        //
        //    }
    }
}
