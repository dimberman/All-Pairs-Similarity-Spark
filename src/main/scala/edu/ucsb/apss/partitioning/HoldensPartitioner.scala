package edu.ucsb.apss.partitioning

import edu.ucsb.apss.partitioning.Partitioner
import edu.ucsb.apss.{BucketAlias, VectorWithNorms}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 12/10/15.
  */
class HoldensPartitioner extends Serializable with Partitioner {
    //    val l1Norm = new Normalizer(p = 1)
    //    val lInfNorm = new Normalizer(p = Double.PositiveInfinity)

    def l1Norm(v: SparseVector) = {
        v.values.map(math.abs).sum
    }


    def lInfNorm(v: SparseVector) = {
        v.values.map(math.abs).max
    }

    def sortByl1Norm(r: RDD[SparseVector]) = {
        r.map(a => (l1Norm(a), a)).sortByKey(true)
    }

    def sortBylinfNorm(r: RDD[SparseVector]) = {
        r.map(
            a => (lInfNorm(a), a)).
          sortByKey(true)
    }


    //TODO split these functions so that the normalization and partitioning are seperated
    def partitionByLinfNorm(r: RDD[SparseVector], numBuckets: Int): RDD[(Int, VectorWithNorms)] = {
        val sorted = sortBylinfNorm(r).map(f => VectorWithNorms(f._1, l1Norm(f._2), f._2))
        sorted.zipWithIndex().map(f => ((f._2 / numBuckets).toInt, f._1))
    }


    def partitionByL1Norm(r: RDD[SparseVector], numBuckets: Int, numVectors: Long): RDD[(Int, VectorWithNorms)] = {
        val a = r.collect()
        val sorted = sortByl1Norm(r).map(f => VectorWithNorms(lInfNorm(f._2), f._1, f._2))
        sorted.zipWithIndex().map { case (vector, index) => ((index / (numVectors / numBuckets)).toInt, vector) }
    }




    def determineBucketLeaders(r: RDD[(Int, VectorWithNorms)]): RDD[(Int, Double)] = {
        r.reduceByKey((a, b) => if (a.l1 > b.l1) a else b).mapValues(_.l1)
    }




    def tieVectorsToHighestBuckets(inputVectors: RDD[(Int, VectorWithNorms)], leaders: Array[(Int, Double)], threshold: Double, sc: SparkContext): RDD[(Int, VectorWithNorms)] = {
        //this step should reduce the amount of data that needs to be shuffled
        val lInfNormsOnly = inputVectors.mapValues(_.lInf)
        //TODO would it be cheaper to pre-shuffle all the vectors into partitions and the mapPartition?
        val broadcastedLeaders = sc.broadcast(leaders)
        val buckets: RDD[Int] = lInfNormsOnly.map {
            case (bucket, norms) =>
                //TODO this is inefficient, can be done in O(logn) time, though it might not be important unless there are LOTS of buckets
                //TODO possibly use Collections.makeBinarySearch?
                val taperedBuckets = broadcastedLeaders.value.take(bucket + 1).toList
                var current = 0
                while ((threshold / norms > taperedBuckets(current)._2) && current < taperedBuckets.size - 1)
                    current = current + 1
                taperedBuckets(current)._1
        }
        inputVectors.zip(buckets).map {
            case ((key, vec), matchedBuckets) =>
                vec.associatedLeader = matchedBuckets
                (key, vec)
        }
    }







//    /**
//      * currently my thought on this function is to have it pre-filter for vectors that are guaranteed dissimilar.
//      * @param bucketValues
//      * @param r
//      * @return
//      */
//    def turnAssignmentsIntoRDDs(bucketValues:List[BucketMapping], r:RDD[(Int, VectorWithNorms)]):List[RDD[((Int, VectorWithNorms), (Int, VectorWithNorms))]] = {
//        //TODO bucketMapping should be in pairs before it gets to this function
//        val pairs = bucketValues.map(a => (a.name, a.values)).flatMap{case (x, b) => b.map(c => (x,c))}
//
//        pairs.map{
//            case (a, b) =>
//                filterPartition(a,r).cartesian(filterPartition(b,r))
////                (a,b) match {
////                    case _ if a < b =>
////                        val bVal = filterPartitionsWithDissimilarity(a,b,r)
////                        r.filter{case(name, vec) => name == a}.cartesian(bVal)
////                    case _ if a > b =>
////                        val aVal = filterPartitionsWithDissimilarity(b,a,r)
////                        r.filter{case(name, vec) => name == b}.cartesian(aVal)
////                    case _ =>
////                        val aVal = r.filter{case(name, vec) => name == a}
////                        aVal.cartesian(aVal)
////                }
//        }
//    }


    def filterPartition[T] = (b:Int, r:RDD[(Int, T)]) => r.filter{case(name, _) => name == b}


    def filterPartitionsWithDissimilarity = (a:Int, b:Int, r:RDD[(Int, VectorWithNorms)]) => r.filter{case(name, vec) => name == b && vec.associatedLeader >= a}



    def calculateSimilarities(r:RDD[((Int, VectorWithNorms), (Int, VectorWithNorms))]) = {

    }



    def pullReleventValues(r: RDD[(Long, (Double, Double, Double, Double))]): RDD[(Long, BucketAlias)] = {
        val b = r.map(a => (a._1, BucketAlias(a._2))).reduceByKey((a, b) => {
            BucketAlias(math.max(a.maxLinf, b.maxLinf), math.min(a.minLinf, b.minLinf), math.max(a.maxL1, b.maxL1), math.min(a.minL1, b.minL1))
        })
        b

    }


}








