package edu.ucsb.apss.util

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * Created by dimberman on 4/28/16.
  */
class PartitionUtilTest extends FlatSpec with Matchers with BeforeAndAfter {
    import edu.ucsb.apss.util.PartitionUtil._

    "clear score array" should "remove all values from the score array and set all values to 0" in {
         val input = Array(1.0,3.0, 4.0)
         clearScoreArray(input)
        input shouldEqual Array(0.0,0.0, 0.0)
     }
}
