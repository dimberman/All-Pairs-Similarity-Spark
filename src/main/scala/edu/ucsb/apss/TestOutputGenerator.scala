package edu.ucsb.apss

import edu.ucsb.apss.preprocessing.TextToVectorConverter
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by dimberman on 4/27/16.
  */
object TestOutputGenerator {

    import edu.ucsb.apss.util.PartitionUtil._

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.yarn.executor.memoryOverhead", "600")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)


        val similarities = run(sc, args(0))
        similarities.saveAsTextFile(args(1))


    }


    def run(sc:SparkContext, path:String)  = {
        val input =  sc.textFile(path)
        val vecs = input.map(BagOfWordToVectorConverter.convert).zipWithIndex
        val BVVec = sc.broadcast(vecs.collect())


        val similarities = vecs.flatMap {
            case (ivec, i) =>
                BVVec.value.map {
                    case (jvec, j) =>
                        val sim = dotProduct(ivec, jvec)
                        ((i, j), sim)
                }
        }.sortByKey()
        similarities
    }
}