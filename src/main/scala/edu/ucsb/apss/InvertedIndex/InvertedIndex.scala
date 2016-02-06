package edu.ucsb.apss.InvertedIndex

import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.VectorWithNorms
import edu.ucsb.apss.holdensDissimilarity.HoldensPSSDriver
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD


/**
  * Created by dimberman on 1/14/16.
  */
case class InvertedIndex(indices: Map[Int, List[FeaturePair]])

object InvertedIndex {


    /**
      * This main is used to create histograms of the inverted indexes for optimization purposes.
      * @param args
      */
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.yarn.executor.memoryOverhead","600")
        val sc = new SparkContext(conf)
        println(s"taking in from ${args(0)}")
        println(s"using  ${args(1)} buckets")
        println(s"threshold set at  ${args(2)}")


        val vectors  =  sc.textFile(args(0)).map(BagOfWordToVectorConverter.convert)
        val numBuckets = args(1).toInt
        val threshold = args(2).toDouble
        createHistogram(sc, vectors, numBuckets, threshold)


    }

    def createHistogram(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double) = {
        //TODO needs refactoring
        val partitioner = new HoldensPSSDriver
        val bucketizedVectors =  partitioner.bucketizeVectors(sc, vectors, numBuckets, threshold)
        val invertedIndexes = generateInvertedIndexes(bucketizedVectors).map{case (key, (ind, bucket)) => (bucket, ind)}
        val numFeatures = invertedIndexes.mapValues(a => a.indices.keySet.size)
        val numFeaturePairs = invertedIndexes.mapValues(a => a.indices.toList.foldRight(0)((a, b) => b + a._2.length))

        val results = numFeatures.join(numFeaturePairs).map{case(key, (nf, nfp)) => s"$key,$nf,$nfp"}.collect()




    }



    def generateInvertedIndexes(bucketizedVectors:RDD[((Int, Int), VectorWithNorms)]):RDD[(Int, (InvertedIndex, (Int, Int)))] = {
        val invInd = bucketizedVectors.map {
            case ((ind, buck), v) => ((ind, buck), createFeaturePairs(v).toMap)
        }.reduceByKey { case (a, b) => mergeMap(a, b)((v1, v2) => v1 ++ v2) }
        val invIndexes = invInd.mapValues(
            a => InvertedIndex(a)
        ).map { case (x, b) => ((x._1 * (x._1 + 1)) / 2 + x._2, (b, x)) }
        invIndexes
    }






    private def merge(a: InvertedIndex, b: InvertedIndex): InvertedIndex = {
        InvertedIndex(mergeMap(a.indices,b.indices)((v1,v2) => v1++v2))
    }

    def addInvertedIndexes:(InvertedIndex, Array[(Int,List[FeaturePair])]) => InvertedIndex = (a, b) => InvertedIndex.merge(a, new InvertedIndex(b.toMap))



    def mergeInvertedIndexes:(InvertedIndex, InvertedIndex) => InvertedIndex = (a, b) => InvertedIndex.merge(a, b)



    def createFeaturePairs(vector:VectorWithNorms) = {
//        val vecto = a
        vector.vector.indices.map(i => (i, List(FeaturePair(vector.index, vector.vector(i)))))
    }

    def apply(a:VectorWithNorms):InvertedIndex = {
        new InvertedIndex(createFeaturePairs(a).toMap)
    }

    def apply(a:List[(Int,List[FeaturePair])])= new InvertedIndex(a.toMap)


    def apply() = {
        new InvertedIndex(Map())
    }

     def mergeMap[A, B](a: Map[A, B], b: Map[A, B])(f: (B, B) => B): Map[A, B] =
        (a /: (for (kv <- b) yield kv)) {
            (c, kv) =>
                c + (if (a.contains(kv._1)) kv._1 -> f(c(kv._1), kv._2) else kv)
        }



    def extractIndexMap(i:InvertedIndex):Map[Long,Int] = {
       i.indices.values.map(a => a.map(_.id)).reduce(_++_).distinct.zipWithIndex.toMap
    }
}


case class FeaturePair(id: Long, weight: Double)