package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.SyncCodeGenTest
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels

import org.apache.spark.broadcast.Broadcast

object Tuple2BroadcastTest extends SyncCodeGenTest[Int, Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()

    val inputClassType1Name = CodeGenUtil.cleanClassName("I")
    val inputClassType2Name = CodeGenUtil.cleanClassName("I")
    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1Name, inputClassType2Name, false)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    val broadcast : Broadcast[Array[Tuple2[Int, Int]]] = null

    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        broadcast.value(in)._1 + broadcast.value(in)._2
      }
    }
  }
}
