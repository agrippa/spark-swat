package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.SparseVectorClassModel

import org.apache.spark.rdd.cl.SparseVectorInputBufferWrapperConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.SparseVector

object SparseVectorBroadcastTest extends CodeGenTest[Int, Double] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val denseVectorModel : SparseVectorClassModel = SparseVectorClassModel.create()
    models.addClassModelFor(classOf[SparseVector], denseVectorModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Double] = {
    val broadcast : Broadcast[Array[SparseVector]] = null

    new Function[Int, Double] {
      override def apply(in : Int) : Double = {
        var sum = 0.0
        var i = 0
        while (i < 5) {
          sum += (broadcast.value(i).values(i) + broadcast.value(i).indices(i))
          i += 1
        }
        sum
      }
    }
  }
}
