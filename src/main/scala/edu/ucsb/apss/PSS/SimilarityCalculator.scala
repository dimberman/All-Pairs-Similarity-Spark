package edu.ucsb.apss.PSS

import edu.ucsb.apss.InvertedIndex.FeaturePair
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 4/28/16.
  */
object SimilarityCalculator extends Serializable {
    def calculateScores(vec:SparseVector, invertedIndex: Map[Int, List[FeaturePair]], indexMap:Map[Long,Int], score:Array[Double]) = {
        vec.indices.zipWithIndex.foreach {
            case (featureIndex, j) =>
                if (invertedIndex.contains(featureIndex)) {
                    val weight_j = vec.values(j)
                    invertedIndex(featureIndex).foreach {
                        case (featurePair) => {
                            val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
                            val l = indexMap(ind_i)
                            score(l) += weight_i * weight_j
                        }
                    }
                }
        }
    }


}
