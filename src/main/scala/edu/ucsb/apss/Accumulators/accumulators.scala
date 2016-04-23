package edu.ucsb.apss.Accumulators

import org.apache.spark.{AccumulatorParam, AccumulableParam}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dimberman on 4/23/16.
  */

case class DebugVal(key: (Int, Int), time: Double, numPairs: Int, numBuckets: Int)

object DebugAcummulatorParam extends AccumulableParam[ArrayBuffer[DebugVal], DebugVal] {
    def zero(value: ArrayBuffer[DebugVal]): ArrayBuffer[DebugVal] = value

    def addInPlace(s1: ArrayBuffer[DebugVal], s2: ArrayBuffer[DebugVal]): ArrayBuffer[DebugVal] = {
        s1 ++= s2
        s1
    }

    def addAccumulator(s1: ArrayBuffer[DebugVal], d: DebugVal) = {
        s1 += d
        s1
    }
}


object LineAcummulatorParam extends AccumulatorParam[String] {
    def zero(value: String): String = value

    def addInPlace(s1: String, s2: String): String = s1 + "\n" + s2
}
