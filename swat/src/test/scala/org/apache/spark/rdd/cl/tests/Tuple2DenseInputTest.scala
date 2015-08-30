package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.cl.DenseVectorInputBufferWrapperConfig

object Tuple2DenseInputTest extends CodeGenTest[(Int, DenseVector), Double] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    val className : String = this.getClass.getSimpleName
    scala.io.Source.fromFile(CodeGenTests.testsPath +
            className.substring(0, className.length - 1) + ".kernel").mkString
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()

    val inputClassType1Name = CodeGenUtil.cleanClassName("I")
    val inputClassType2Name = CodeGenUtil.cleanClassName("org.apache.spark.mllib.linalg.DenseVector")
    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        inputClassType1Name, inputClassType2Name, false)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)

    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create(
            DenseVectorInputBufferWrapperConfig.tiling)
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(0).addTypeParameter("I", false)
    params.get(0).addTypeParameter("Lorg.apache.spark.mllib.linalg.DenseVector;", true)
  }

  def getFunction() : Function1[(Int, DenseVector), Double] = {
    new Function[(Int, DenseVector), Double] {
      override def apply(in : (Int, DenseVector)) : Double = {
        in._2(in._1 - in._2.size - 4)
      }
    }
  }
}
