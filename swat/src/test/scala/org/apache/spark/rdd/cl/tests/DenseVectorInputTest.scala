package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel

import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil

import org.apache.spark.mllib.linalg.DenseVector

object DenseVectorInputTest extends CodeGenTest[DenseVector, Double] {
  def getExpectedKernel() : String = {
    "#pragma OPENCL EXTENSION cl_khr_fp64 : enable\n" +
    "\n" +
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, unsigned int heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "\n" +
    "typedef struct __attribute__ ((packed)) org_apache_spark_mllib_linalg_DenseVector_s{\n" +
    "   __global double*  values;\n" +
    "   int  size;\n" +
    "   \n" +
    "} org_apache_spark_mllib_linalg_DenseVector;\n" +
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
    "static double org_apache_spark_rdd_cl_tests_DenseVectorInputTest$$anon$1__apply(This *this, __global org_apache_spark_mllib_linalg_DenseVector* in){\n" +
    "   return(\n" +
    "   {\n" +
    "      double sum = 0.0;\n" +
    "      int i = 0;\n" +
    "      for (; i<org_apache_spark_mllib_linalg_DenseVector__size(in); i = i + 1){\n" +
    "         sum = sum + org_apache_spark_mllib_linalg_DenseVector__apply(in, i);\n" +
    "      }\n" +
    "      sum;\n" +
    "   });\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global org_apache_spark_mllib_linalg_DenseVector* in0, __global double *in0_values, __global int *in0_sizes, __global int *in0_offsets, \n" +
    "      __global double* out, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   __global org_apache_spark_mllib_linalg_DenseVector *my_in0 = in0 + get_global_id(0);\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      my_in0->values = in0_values + in0_offsets[i];\n" +
    "      my_in0->size = in0_sizes[i];\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_DenseVectorInputTest$$anon$1__apply(this, in0 + i);\n" +
    "      \n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create()
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[DenseVector, Double] = {
    new Function[DenseVector, Double] {
      override def apply(in : DenseVector) : Double = {
        var sum = 0.0
        var i = 0
        while (i < in.size) {
            sum += in(i)
            i += 1
        }
        sum
      }
    }
  }
}
