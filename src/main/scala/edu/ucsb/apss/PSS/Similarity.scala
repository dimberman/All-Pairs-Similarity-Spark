package edu.ucsb.apss.PSS

/**
  * Created by dimberman on 1/31/16.
  */
case class Similarity(i:Long, j:Long, similarity:Double) extends Ordering[Similarity]{
    override def compare(x: Similarity, y: Similarity): Int = x.similarity compare y.similarity
}

object Similarity{
    implicit def orderingBySimilarity[A <: Similarity]: Ordering[A] =
        Ordering.by(e => e.similarity)
}