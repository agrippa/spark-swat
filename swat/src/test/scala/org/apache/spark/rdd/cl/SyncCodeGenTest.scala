package org.apache.spark.rdd.cl

import java.io.IOException
import java.util.LinkedList
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.writer.ScalaArrayParameter

trait SyncCodeGenTest[P, R] extends CodeGenTest[R] {
  def getFunction() : Function1[P, R]
}

