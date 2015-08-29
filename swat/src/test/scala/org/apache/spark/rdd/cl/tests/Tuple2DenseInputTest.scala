package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.cl.DenseVectorInputBufferWrapperConfig

object Tuple2DenseInputTest extends CodeGenTest[(Int, DenseVector), Double] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    "#pragma OPENCL EXTENSION cl_khr_fp64 : enable\n" +
    "\n" +
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, unsigned int heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint rounded = nbytes + (8 - (nbytes % 8));\n" +
    "   uint offset = atomic_add(free_index, rounded);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "\n" +
    "typedef struct __attribute__ ((packed)) org_apache_spark_mllib_linalg_DenseVector_s{\n" +
    "   __global double*  values;\n" +
    "   int  size;\n" +
    "   \n" +
    "} org_apache_spark_mllib_linalg_DenseVector;\n" +
    "\n" +
    "typedef struct __attribute__ ((packed)) scala_Tuple2_I_org_apache_spark_mllib_linalg_DenseVector_s{\n" +
    "   __global org_apache_spark_mllib_linalg_DenseVector  * _2;\n" +
    "   int  _1;\n" +
    "   \n" +
    "} scala_Tuple2_I_org_apache_spark_mllib_linalg_DenseVector;\n" +
    "typedef struct This_s{\n" +
    "   } This;\n" +
    "\n" +
    "static int org_apache_spark_mllib_linalg_DenseVector__size(__global org_apache_spark_mllib_linalg_DenseVector *this) {\n" +
    "    return (this->size);\n" +
    "}\n" +
    "\n" +
    "\n" +
    "static double org_apache_spark_mllib_linalg_DenseVector__apply(__global org_apache_spark_mllib_linalg_DenseVector *this, int index) {\n" +
    "    return (this->values)[32 * index];\n" +
    "}\n" +
    "\n" +
    "static double org_apache_spark_rdd_cl_tests_Tuple2DenseInputTest$$anon$1__apply(This *this, __global scala_Tuple2_I_org_apache_spark_mllib_linalg_DenseVector* in){\n" +
    "\n" +
    "   return(org_apache_spark_mllib_linalg_DenseVector__apply(in->_2, ((in->_1 - org_apache_spark_mllib_linalg_DenseVector__size(in->_2)) - 4)));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int * in0_1, __global org_apache_spark_mllib_linalg_DenseVector* in0_2, __global double *in0_2_values, __global int *in0_2_sizes, __global int *in0_2_offsets, int nin0_2, __global scala_Tuple2_I_org_apache_spark_mllib_linalg_DenseVector *in0, \n" +
    "      __global double* out, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   __global scala_Tuple2_I_org_apache_spark_mllib_linalg_DenseVector *my_in0 = in0 + get_global_id(0);\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      my_in0->_1 = in0_1[i]; my_in0->_2 = in0_2 + i; my_in0->_2->values = in0_2_values + in0_2_offsets[i]; my_in0->_2->size = in0_2_sizes[i];\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_Tuple2DenseInputTest$$anon$1__apply(this, my_in0);\n" +
    "      \n" +
    "   }\n" +
    "}\n"
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
