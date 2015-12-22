package org.apache.spark.rdd.cl

import java.io.IOException
import java.util.LinkedList
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.writer.ScalaArrayParameter

trait AsyncCodeGenTest[R] extends CodeGenTest[R] {
  def getFunction() : Function0[R]
}
