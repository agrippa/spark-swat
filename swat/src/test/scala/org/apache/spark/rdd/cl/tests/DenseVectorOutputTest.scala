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
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.rdd.cl.DenseVectorInputBufferWrapperConfig

object DenseVectorOutputTest extends CodeGenTest[Int, DenseVector] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    "#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics : enable\n" +
    "#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics : enable\n" +
    "#pragma OPENCL EXTENSION cl_khr_local_int32_base_atomics : enable\n" +
    "#pragma OPENCL EXTENSION cl_khr_local_int32_extended_atomics : enable\n" +
    "static int atomicAdd(__global int *_arr, int _index, int _delta){\n" +
    "   return atomic_add(&_arr[_index], _delta);\n" +
    "}\n" +
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
    "   __global void *heap;\n" +
    "   __global uint *free_index;\n" +
    "   int alloc_failed;\n" +
    "   unsigned int heap_size;\n" +
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
    "static __global org_apache_spark_mllib_linalg_DenseVector *org_apache_spark_rdd_cl_tests_DenseVectorOutputTest$$anon$1__apply(This *this, int in){\n" +
    "\n" +
    "   return(\n" +
    "   {\n" +
    "        __global double * __alloc0 = (__global double *)alloc(this->heap, this->free_index, this->heap_size, sizeof(int) + (sizeof(double) * (in)), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); } *((__global int *)__alloc0) = (in); __alloc0 = (__global double *)(((__global int *)__alloc0) + 1); \n" +
    " \n" +
    "       __global double* valuesArr = __alloc0;\n" +
    "      int i = 0;\n" +
    "      for (; i<in; i = i + 1){\n" +
    "      \n" +
    "         valuesArr[i]  = (double)(2 * i);\n" +
    "      }\n" +
    "      __global org_apache_spark_mllib_linalg_DenseVector * __alloc1 = (__global org_apache_spark_mllib_linalg_DenseVector *)alloc(this->heap, this->free_index, this->heap_size, sizeof(org_apache_spark_mllib_linalg_DenseVector), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); }\n" +
    "      ({ __alloc1->values = valuesArr; __alloc1->size = *(((__global int *)__alloc1->values) - 1); __alloc1; });\n" +
    "   });\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global org_apache_spark_mllib_linalg_DenseVector* out, __global void *heap, __global uint *free_index, unsigned int heap_size, __global int *processing_succeeded, __global int *any_failed, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   this->heap = heap;\n" +
    "   this->free_index = free_index;\n" +
    "   this->heap_size = heap_size;\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      if (processing_succeeded[i]) continue;\n" +
    "      \n" +
    "      this->alloc_failed = 0;\n" +
    "      __global org_apache_spark_mllib_linalg_DenseVector* result = org_apache_spark_rdd_cl_tests_DenseVectorOutputTest$$anon$1__apply(this, in0[i]);\n" +
    "      if (this->alloc_failed) {\n" +
    "         processing_succeeded[i] = 0;\n" +
    "         *any_failed = 1;\n" +
    "      } else {\n" +
    "         processing_succeeded[i] = 1;\n" +
    "         result->values = ((__global char *)result->values) - ((__global char *)this->heap);\n" +
    "         out[i] = *result;\n" +
    "      }\n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val denseVectorModel : DenseVectorClassModel = DenseVectorClassModel.create(
            DenseVectorInputBufferWrapperConfig.tiling)
    models.addClassModelFor(classOf[DenseVector], denseVectorModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[Int, DenseVector] = {
    new Function[Int, DenseVector] {
      override def apply(in : Int) : DenseVector = {
        val valuesArr = new Array[Double](in)

        var i = 0
        while (i < in) {
          valuesArr(i) = 2 * i
          i += 1
        }
        Vectors.dense(valuesArr).asInstanceOf[DenseVector]
      }
    }
  }
}
