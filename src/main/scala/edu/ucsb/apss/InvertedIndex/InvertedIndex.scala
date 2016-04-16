package edu.ucsb.apss.InvertedIndex

import java.io.{File, PrintWriter}

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import edu.ucsb.apss.InvertedIndex.InvertedIndex.MaxMap
import edu.ucsb.apss.VectorWithNorms
import edu.ucsb.apss.VectorWithNorms
import edu.ucsb.apss.holdensDissimilarity.HoldensPSSDriver
import edu.ucsb.apss.partitioning.HoldensPartitioner
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import edu.ucsb.apss.util.PartitionUtil.VectorWithNorms
import org.apache.log4j.Logger
import org.apache.spark.{AccumulatorParam, Accumulator, SparkConf, SparkContext}
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

import scala.util.Random


/**
  * Created by dimberman on 1/14/16.
  */


case class InvertedIndex(indices: Map[Int, List[FeaturePair]], bucket: Int = -1, tl: Int = -1, maxMap:MaxMap = MMap())

object InvertedIndex {
    type IndexMap = MMap[Int, List[FeaturePair]]
    type MaxMap = MMap[Long, Double]

    import HoldensPartitioner._

    type Bucket = (Int, Int)

    val log = Logger.getLogger(this.getClass)

    /**
      * This main is used to create histograms of the inverted indexes for optimization purposes.
      * @param args
      */
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.yarn.executor.memoryOverhead", "600")
        val sc = new SparkContext(conf)
        run(args, sc)

    }

    def run(args: Array[String], sc: SparkContext) = {
        log.info(s"taking in from ${args(0)}")
        log.info(s"using  ${args(1)} buckets")
        log.info(s"threshold set at  ${args(2)}")
        log.info(s"outputting to  ${args(3)}")


        val numBuckets = args(1).toInt
        val threshold = args(2).toDouble
        //        val g = 5
        val input = sc.textFile(args(0))
        val converter = new TweetToVectorConverter
        val vectors = input.map(converter.convertTweetToVector)

        createHistogram(sc, vectors, numBuckets, threshold, args(3))
        //        results.saveAsTextFile(args(3))
    }


    def createHistogram(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double, outputPath: String) = {
        val bucketName = "apss-masters"
        val folderName = s"inverted-index/2-20-2016/b$numBuckets-t$threshold"
        val partitionDriver = new HoldensPSSDriver
        val partitioner =  HoldensPartitioner

        val s3client = new AmazonS3Client()

        val count = vectors.count()
        val l1partitionedVectors = partitionByL1Sort(recordIndex(vectors), numBuckets, count)
        val parts = l1partitionedVectors.countByKey()
        val partitionSizes = parts.toList.sortBy(_._1).map(_.toString())
        val partitionFile = writeFile("partitionSizes.csv", partitionSizes)
        putInS3(s3client, bucketName, folderName + "/partition-sizes/", partitionFile)


        val bucketizedVectors = partitionDriver.bucketizeVectors(sc, vectors, numBuckets, threshold).repartition(15).persist()
        val bucketizedVectorMap = bucketizedVectors.countByKey()
        val bucketizedVectorSizes = bucketizedVectorMap.toList.map { case ((buck, tv), v) => s"($buck-$tv), $v" }
        val bucketFile = writeFile("bucketSizes.csv", bucketizedVectorSizes)
        putInS3(s3client, bucketName, folderName + "/bucket-sizes/", bucketFile)


        val needsSplitting = bucketizedVectors.countByKey().filter(_._2 > 2500).map { case ((a, b), c) => ((a.toInt, b.toInt), c) }.toMap
        val rNumBuckets = (numBuckets * (numBuckets + 1)) / 2

        val invertedIndexes = generateInvertedIndexes(bucketizedVectors, needsSplitting, rNumBuckets).map { case (key, ind) => ((ind.bucket, ind.tl), ind) }
        val numFeatures = invertedIndexes.mapValues(a => a.indices.keySet.size)
        val numFeaturePairs = invertedIndexes.mapValues(a => a.indices.toList.foldRight(0)((a, b) => b + a._2.length))

        val results = numFeatures.join(numFeaturePairs).map { case ((buck, tv), (nf, nfp)) => s"($buck-$tv),$nf,$nfp" }.collect().toList
        val resultFile = writeFile("InvInd.csv", results)
        putInS3(s3client, bucketName, folderName + "/InvInd-sizes/", resultFile)
    }


    private def writeFile(name: String, i: List[String]): File = {
        val f = new File(name)
        val writer = new PrintWriter(f)
        i.foreach(s => writer.write(s + "\n"))
        writer.close()
        f

    }

    private def putInS3(s3Client: AmazonS3Client, bucketName: String, folder: String, file: File) = {
        val request = new PutObjectRequest(bucketName, folder + file.getName, file)
        s3Client.putObject(request)
    }


    def generateInvertedIndexes(bucketizedVectors: RDD[((Int, Int), VectorWithNorms)], needsSplitting: Map[(Int, Int), Long] = Map(), numParts:Int): RDD[((Int,Int), InvertedIndex)] = {


        val incorrectAccum:Accumulator [ArrayBuffer[String]] = bucketizedVectors.context.accumulator(ArrayBuffer(""))(StringAccumulatorParam)
        val splitFeaturePairs:RDD[((Int,Int), (IndexMap, MaxMap, Bucket))] = splitBucketizedVectors(bucketizedVectors, needsSplitting, numParts)

        val mergedFeaturePairs = splitFeaturePairs.reduceByKey{
            case((map1, info1, idx1),(map2, info2, idx2)) => {
                //                val (map1, idx1, map2, idx2) = (a._1, a._2, b._1, b._2)
                for (k <- map2.keys) {
                    if (map1.contains(k)) map1(k) = map1(k) ++ map2(k)
                    else map1 += (k -> map2(k))
                }
                val mergedInfo = info1 ++ info2
                if(idx1 != idx2) incorrectAccum +=  ArrayBuffer(s"index overlap shouldn't happen. values: $idx1, $idx2\n")
                //                require(idx1 == idx2, s"Values with different buckets have been given the same index. This shouldn't happen. values: $idx1, $idx2")
                (map1, mergedInfo, idx1)
            }

        }

        incorrectAccum.value.tail.foreach(log.error(_))

        require(incorrectAccum.value.length < 2, "there were incorrectly partitioned inverted index values")

        mergedFeaturePairs.mapValues { case (a, m, (buck, tl)) => new InvertedIndex(a.toMap, buck, tl, m) }
    }



    def splitBucketizedVectors(bucketizedVectors: RDD[((Int, Int), VectorWithNorms)],  needsSplitting: Map[(Int, Int), Long] = Map(), numParts:Int): RDD[((Int,Int), (IndexMap, MaxMap, Bucket))] = {
        bucketizedVectors.map {
            case (x, v) => {
                val featureMap: IndexMap = MMap[Int, List[FeaturePair]]() ++= createFeaturePairs(v).toMap
                val max:MaxMap = MMap[Long,Double]() += (v.index -> v.lInf)
                (x, (featureMap, max, x))
            }
        }
    }

    def deriveID(x:(Int, Int), needsSplitting: Map[(Int, Int), Long] = Map(), numParts:Int):Int = {
        val addition = if (needsSplitting.contains(x)) numParts * (Random.nextInt() % 2) + 1 else 0
        (x._1 * (x._1 + 1)) / 2 + x._2  + addition
    }


    def mergeFeaturePairs(a: (IndexMap, Bucket), b: (IndexMap, Bucket)): (IndexMap, Bucket) = {
        val (map1, idx1, map2, idx2) = (a._1, a._2, b._1, b._2)
            for (k <- map2.keys) {
                if (map1.contains(k)) map1(k) = map1(k) ++ map2(k)
                else map1 += (k -> map2(k))
            }
            require(idx1 == idx2, s"Values with different buckets have been given the same index. This shouldn't happen. values: $idx1, $idx2")
            (map1, idx1)
    }


    def createFeaturePairs(vector: VectorWithNorms): Array[(Int, List[FeaturePair])] = {
        vector.vector.indices.map(i => (i, List(FeaturePair(vector.index, vector.vector(i)))))
    }

    def apply(a: VectorWithNorms): InvertedIndex = {
        new InvertedIndex(createFeaturePairs(a).toMap)
    }

    def apply(a: List[(Int, List[FeaturePair])], buck: Int, tl: Int) = new InvertedIndex(a.toMap, buck, tl)

    def apply(a: List[(Int, List[FeaturePair])]) = new InvertedIndex(a.toMap)


    def apply() = {
        new InvertedIndex(Map())
    }

    def mergeMap[A, B](a: Map[A, B], b: Map[A, B])(f: (B, B) => B): Map[A, B] =
        (a /: (for (kv <- b) yield kv)) {
            (c, kv) =>
                c + (if (a.contains(kv._1)) kv._1 -> f(c(kv._1), kv._2) else kv)
        }


    def extractIndexMap(i: InvertedIndex): Map[Long, Int] = {
        i.indices.values.map(a => a.map(_.id)).reduce(_ ++ _).distinct.zipWithIndex.toMap
    }
}


case class FeaturePair(id: Long, weight: Double)


object StringAccumulatorParam extends AccumulatorParam[ArrayBuffer[String]] {

    def zero(initialValue:  ArrayBuffer[String]): ArrayBuffer[String] = {
        ArrayBuffer("")
    }

    def addInPlace(s1: ArrayBuffer[String], s2: ArrayBuffer[String]): ArrayBuffer[String] = {
        if(s1.length + s2.length < 2000)
            s1++s2
        else s1
    }
}