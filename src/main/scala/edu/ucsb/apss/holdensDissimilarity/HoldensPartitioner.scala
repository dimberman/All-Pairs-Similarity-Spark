package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.{BucketMapping, BucketAlias, VectorWithNorms}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

/**
  * Created by dimberman on 12/10/15.
  */
class HoldensPartitioner extends Serializable {
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
        r.map(a => (lInfNorm(a), a)).sortByKey(true)
    }


    //TODO split these functions so that the normalization and partitioning are seperated
    def partitionByLinfNorm(r: RDD[SparseVector], numBuckets: Int): RDD[(Int, VectorWithNorms)] = {
        val sorted = sortBylinfNorm(r).map(f => VectorWithNorms(f._1, l1Norm(f._2), f._2))
        sorted.zipWithIndex().map(f => ((f._2 / numBuckets).toInt, f._1))
    }


    def partitionByL1Norm(r: RDD[SparseVector], numBuckets: Int, numVectors: Long): RDD[(Int, VectorWithNorms)] = {
        val sorted = sortByl1Norm(r).map(f => VectorWithNorms(lInfNorm(f._2), f._1, f._2))
        sorted.zipWithIndex().map { case (vector, index) => ((index / (numVectors / numBuckets)).toInt, vector) }
    }


    /**
      * This function will allow me to partition based on the BucketMappings and perform manual load balancing.
      * The problem I was running into before was that it was impossible to dynamically partition a value to multiple partitions.
      * To solve this, the following function flatmaps the values with a "PartitionKey" which can then be mapped via a repartition.
      * @param r
      * @param bucketValues
      * @return
      */
    def prepareTasksForParallelization(r:RDD[(Int, VectorWithNorms)], bucketValues:List[BucketMapping]):RDD[(Int, (Int, VectorWithNorms))] = {
        val BVBucketValues = r.context.broadcast(bucketValues)
        r.flatMap {
            case(bucket, vec) =>
                BVBucketValues.value.flatMap(m => if (m.values.contains(bucket)) Some((m.name, (bucket, vec))) else None)
        }
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


    def createPartitioningAssignments(numBuckets: Int): List[BucketMapping] = {
        val masters: List[Int] = List.range(0, numBuckets)
        masters.map(
            m =>
                numBuckets % 2 match {
                    case 1 =>
                        val e = List.range(m + 1, (m + 1) + (numBuckets - 1) / 2)
                        val c = e.map(_ % numBuckets)
                        BucketMapping(m, c)
                    case 0 =>
                        if (m < numBuckets / 2)
                            BucketMapping(m, List.range(m + 1, (m + 1) + numBuckets / 2).map(_ % numBuckets))
                        else {
                            val x = (m + 1)  + numBuckets / 2 - 1
                            val e = List.range(m + 1, x)
                            val c = e.map(_ % numBuckets)
                            BucketMapping(m, c)
                        }
                }
        )
    }

    /**
      * currently my thought on this function is to have it pre-filter for vectors that are guaranteed dissimilar.
      * @param bucketValues
      * @param r
      * @return
      */
    def turnAssignmentsIntoRDDs(bucketValues:List[BucketMapping], r:RDD[(Int, VectorWithNorms)]):List[RDD[((Int, VectorWithNorms), (Int, VectorWithNorms))]] = {
        //TODO bucketMapping should be in pairs before it gets to this function
        val pairs = bucketValues.map(a => (a.name, a.values)).flatMap{case (x, b) => b.map(c => (x,c))}

        pairs.map{
            case (a, b) =>
                filterPartition(a,r).cartesian(filterPartition(b,r))
//                (a,b) match {
//                    case _ if a < b =>
//                        val bVal = filterPartitionsWithDissimilarity(a,b,r)
//                        r.filter{case(name, vec) => name == a}.cartesian(bVal)
//                    case _ if a > b =>
//                        val aVal = filterPartitionsWithDissimilarity(b,a,r)
//                        r.filter{case(name, vec) => name == b}.cartesian(aVal)
//                    case _ =>
//                        val aVal = r.filter{case(name, vec) => name == a}
//                        aVal.cartesian(aVal)
//                }
        }
    }


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








