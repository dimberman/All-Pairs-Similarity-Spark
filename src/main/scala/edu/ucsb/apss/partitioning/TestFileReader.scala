package edu.ucsb.apss.partitioning

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox
/**
  * Created by dimberman on 5/8/16.
  */
class TestFileReader {
    def readString() = {
        val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
        val parsed =  toolbox.parse("2*3+16")
        val compe = toolbox.eval(parsed).asInstanceOf[Int]
        compe
    }

}
