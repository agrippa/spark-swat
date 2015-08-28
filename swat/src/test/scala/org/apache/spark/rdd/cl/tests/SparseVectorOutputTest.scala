package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.SparseVectorClassModel

import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.rdd.cl.SparseVectorInputBufferWrapperConfig

object SparseVectorOutputTest extends CodeGenTest[Int, SparseVector] {
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
    "   __global void *heap;\n" +
    "   __global uint *free_index;\n" +
    "   int alloc_failed;\n" +
    "   unsigned int heap_size;\n" +
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
    "static __global org_apache_spark_mllib_linalg_SparseVector *org_apache_spark_rdd_cl_tests_SparseVectorOutputTest$$anon$1__apply(This *this, int in){\n" +
    "\n" +
    "   return(\n" +
    "   {\n" +
    "   \n" +
    "      __global int * __alloc0 = (__global int *)alloc(this->heap, this->free_index, this->heap_size, sizeof(long) + (sizeof(int) * (in)), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); } *((__global long *)__alloc0) = (in); __alloc0 = (__global int *)(((__global long *)__alloc0) + 1); \n" +
    "       __global int* indicesArr = __alloc0;\n" +
    "      __global double * __alloc1 = (__global double *)alloc(this->heap, this->free_index, this->heap_size, sizeof(long) + (sizeof(double) * (in)), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); } *((__global long *)__alloc1) = (in); __alloc1 = (__global double *)(((__global long *)__alloc1) + 1); \n" +
    "       __global double* valuesArr = __alloc1;\n" +
    "      int i = 0;\n" +
    "      for (; i<in; i = i + 1){\n" +
    "      \n" +
    "         indicesArr[i]  = 10 * in;\n" +
    "         valuesArr[i]  = (double)(20 * in);\n" +
    "      }\n" +
    "      __global org_apache_spark_mllib_linalg_SparseVector * __alloc2 = (__global org_apache_spark_mllib_linalg_SparseVector *)alloc(this->heap, this->free_index, this->heap_size, sizeof(org_apache_spark_mllib_linalg_SparseVector), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); }\n" +
    "      ({ __alloc2->size = in; __alloc2->indices = indicesArr; __alloc2->values = valuesArr; __alloc2; });\n" +
    "   });\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global org_apache_spark_mllib_linalg_SparseVector* out, __global void *heap, __global uint *free_index, unsigned int heap_size, __global int *processing_succeeded, __global int *any_failed, int N) {\n" +
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
    "      __global org_apache_spark_mllib_linalg_SparseVector* result = org_apache_spark_rdd_cl_tests_SparseVectorOutputTest$$anon$1__apply(this, in0[i]);\n" +
    "      if (this->alloc_failed) {\n" +
    "         processing_succeeded[i] = 0;\n" +
    "         *any_failed = 1;\n" +
    "      } else {\n" +
    "         processing_succeeded[i] = 1;\n" +
    "         result->values = ((__global char *)result->values) - ((__global char *)this->heap);\n" +
    "         result->indices = ((__global char *)result->indices) - ((__global char *)this->heap);\n" +
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
    val sparseVectorModel : SparseVectorClassModel =
            SparseVectorClassModel.create(
                    SparseVectorInputBufferWrapperConfig.tiling)
    models.addClassModelFor(classOf[SparseVector], sparseVectorModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[Int, SparseVector] = {
    new Function[Int, SparseVector] {
      override def apply(in : Int) : SparseVector = {
        val indicesArr = new Array[Int](in)
        val valuesArr = new Array[Double](in)

        var i = 0
        while (i < in) {
          indicesArr(i) = 10 * in
          valuesArr(i) = 20 * in
          i += 1
        }
        Vectors.sparse(in, indicesArr, valuesArr).asInstanceOf[SparseVector]
      }
    }
  }
}
