package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.InvertedIndex.{FeaturePair, InvertedIndex}
import edu.ucsb.apss.tokenization1.BagOfWordToVectorConverter
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by dimberman on 1/3/16.
  */
class HoldensPSSDriver {
    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double) = {
        val count = vectors.count
        val partitioner = new HoldensPartitioner

        val partitionedVectors = partitioner.partitionByL1Norm(vectors, numBuckets, count).persist()
        val bucketLeaders = partitioner.determineBucketLeaders(partitionedVectors).collect()
        //TODO should I modify this so that it uses immutable objects?
        partitioner.tieVectorsToHighestBuckets(partitionedVectors, bucketLeaders, threshold, sc)

        val invIndexes = partitionedVectors.map { case (a, v) => (a, InvertedIndex.createFeaturePairs(a, v)) }
          //TODO it would be more efficient to not create a new object for every add
          .aggregateByKey(InvertedIndex())(
            addInvertedIndexes,
            mergeInvertedIndexes
        ).map { case (a, b) => (a, (b, a)) }
        //TODO Save inverted indices to hbase
        //invIndexes.saveAsHadoopDataset()


        val assignments = partitioner.createPartitioningAssignments(numBuckets)
        //TODO test that this will guarantee that all key values will be placed into a single partition
        //TODO this function would be the perfect point to filter the values via static partitioning
        val partitionedTasks = partitioner.prepareTasksForParallelization(partitionedVectors, assignments).groupByKey().join(invIndexes)


        val a = partitionedTasks.mapValues {
            case (vecs, (i, bucket)) =>
                val invertedIndex = i.indices
                vecs.map {
                    case (buck, v) =>
                        val scores = Array[Double](vecs.size)
                        var r = v.l1
                        val d_i = invertedIndex.filter(a => v.vector.indices.contains(a._1))
                        val d_j = v.vector.indices.flatMap(ind => if (d_i.contains(ind)) Some((ind, v.vector.values(ind))) else None)
                        d_j.foreach {
                            case (ind_j, weight_j) =>
                                d_i(ind_j).foreach {
                                    case (featurePair) => {
                                        val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
                                        if (!(scores(ind_i) + v.lInf * r < threshold)) scores(ind_j) += weight_i * weight_j
                                    }
                                }
                                r -= weight_j
                        }
                        scores.zipWithIndex.filter(_._1>threshold).map{case(score, ind_i) => (ind_i, buck, score)}
                }
        }



        def addInvertedIndexes:(InvertedIndex, Array[(Int,List[FeaturePair])]) => InvertedIndex = (a, b) => InvertedIndex.merge(a, new InvertedIndex(b.toMap))
        def mergeInvertedIndexes:(InvertedIndex, InvertedIndex) => InvertedIndex = (a, b) => InvertedIndex.merge(a, b)


        //        partitionedTasks.mapPartitions(
        //            iter => {
        //                var a = 0
        //                val b = iter.toStream
        //                b.flatMap(
        //                    summarizedRow => {
        //                        val row = summarizedRow.
        //                        val buf = new ListBuffer[((Int, Int), Double)]()
        //                        b.drop(a+1).foreach(
        //                            summarizedRow2 => {
        //                                val row2 = summarizedRow2.row
        //                                if(summarizedRow.summary.tmax < summarizedRow2.summary.colSum){
        //                                    val c = ((summarizedRow.summary.index, summarizedRow2.summary.index), calculateCosineSimilarity(row, row2))
        //                                    buf += c
        //                                }
        //                            }
        //                        )
        //                        a+=1
        //                        buf
        //                    }
        //                ).toIterator
        //            }
        //
        //        )
        //        val c = sims.collect()
        //        println("sadg")
        //        //        val rSim = sims.reduceByKey(_ + _)
        //        val mSim = sims.map { case ((i, j), sim) =>
        //            MatrixEntry(i.toLong, j.toLong, sim)
        //        }
        //        new CoordinateMatrix(mSim, n, n)
        //    }
    }
}
