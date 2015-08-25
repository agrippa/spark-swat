package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.SparseVectorClassModel

import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil

import org.apache.spark.mllib.linalg.SparseVector

import org.apache.spark.rdd.cl.SparseVectorInputBufferWrapperConfig

object SparseVectorAssignTest extends CodeGenTest[SparseVector, (Int, Double)] {
  def getExpectedException() : String = {
    return "Assigning from the indices or values of a MLLib Sparse Vector is " +
        "not supported"
  }

  def getExpectedKernel() : String = {
      ""
  }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()

    val sparseVectorModel : SparseVectorClassModel =
            SparseVectorClassModel.create(
                    SparseVectorInputBufferWrapperConfig.tiling)
    models.addClassModelFor(classOf[SparseVector], sparseVectorModel)

    val outputClassType1Name = CodeGenUtil.cleanClassName("I")
    val outputClassType2Name = CodeGenUtil.cleanClassName("D")
    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        outputClassType1Name, outputClassType2Name, true)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(1).addTypeParameter("I", false)
    params.get(1).addTypeParameter("D", false)
  }

  def getFunction() : Function1[SparseVector, (Int, Double)] = {
    new Function[SparseVector, (Int, Double)] {
      override def apply(in : SparseVector) : (Int, Double) = {
        val indexArr = in.indices
        val valueArr = in.values
        (indexArr(0), valueArr(0))
      }
    }
  }
}
