package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.SparseVectorClassModel

import org.apache.spark.rdd.cl.SparseVectorInputBufferWrapperConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.SparseVector

object SparseVectorBroadcastTest extends CodeGenTest[Int, Double] {
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
    "typedef struct __attribute__ ((packed)) org_apache_spark_mllib_linalg_SparseVector_s{\n" +
    "   __global int*  indices;\n" +
    "   __global double*  values;\n" +
    "   int  size;\n" +
    "   \n" +
    "} org_apache_spark_mllib_linalg_SparseVector;\n" +
    "typedef struct This_s{\n" +
    "   __global org_apache_spark_mllib_linalg_SparseVector *broadcast$1; ;\n" +
    "   } This;\n" +
    "\n" +
    "static int org_apache_spark_mllib_linalg_SparseVector__size(__global org_apache_spark_mllib_linalg_SparseVector *this) {\n" +
    "    return (this->size);\n" +
    "}\n" +
    "\n" +
    "\n" +
    "static __global int *org_apache_spark_mllib_linalg_SparseVector__indices(__global org_apache_spark_mllib_linalg_SparseVector *this) {\n" +
    "    return (this->indices);\n" +
    "}\n" +
    "\n" +
    "\n" +
    "static __global double *org_apache_spark_mllib_linalg_SparseVector__values(__global org_apache_spark_mllib_linalg_SparseVector *this) {\n" +
    "    return (this->values);\n" +
    "}\n" +
    "\n" +
    "static double org_apache_spark_rdd_cl_tests_SparseVectorBroadcastTest$$anon$1__apply$mcDI$sp(This *this, int in){\n" +
    "\n" +
    "   return(\n" +
    "   {\n" +
    "   \n" +
    "      double sum = 0.0;\n" +
    "      int i = 0;\n" +
    "      for (; i<5; i = i + 1){\n" +
    "      \n" +
    "         sum = sum + (org_apache_spark_mllib_linalg_SparseVector__values( &(this->broadcast$1[i]))[32 * (i)] + (double)org_apache_spark_mllib_linalg_SparseVector__indices( &(this->broadcast$1[i]))[32 * (i)]);\n" +
    "      }\n" +
    "      sum;\n" +
    "   });\n" +
    "}\n" +
    "static double org_apache_spark_rdd_cl_tests_SparseVectorBroadcastTest$$anon$1__apply(This *this, int in){\n" +
    "\n" +
    "   return(org_apache_spark_rdd_cl_tests_SparseVectorBroadcastTest$$anon$1__apply$mcDI$sp(this, in));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global double* out, __global org_apache_spark_mllib_linalg_SparseVector* broadcast$1, __global int *broadcast$1_indices, __global double *broadcast$1_values, __global int *broadcast$1_sizes, __global int *broadcast$1_offsets, int nbroadcast$1, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   this->broadcast$1 = broadcast$1;\n" +
    "   for (int j = 0; j < nbroadcast$1; j++) {\n" +
    "      (this->broadcast$1)[j].values = broadcast$1_values + broadcast$1_offsets[j];\n" +
    "      (this->broadcast$1)[j].indices = broadcast$1_indices + broadcast$1_offsets[j];\n" +
    "      (this->broadcast$1)[j].size = broadcast$1_sizes[j];\n" +
    "   }\n" +
    ";\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_SparseVectorBroadcastTest$$anon$1__apply(this, in0[i]);\n" +
    "      \n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val denseVectorModel : SparseVectorClassModel = SparseVectorClassModel.create(
            SparseVectorInputBufferWrapperConfig.tiling)
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
