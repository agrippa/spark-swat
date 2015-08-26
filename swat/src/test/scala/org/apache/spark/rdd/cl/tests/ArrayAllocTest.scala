package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

object ArrayAllocTest extends CodeGenTest[Int, Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    "#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics : enable\n" +
    "#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics : enable\n" +
    "#pragma OPENCL EXTENSION cl_khr_local_int32_base_atomics : enable\n" +
    "#pragma OPENCL EXTENSION cl_khr_local_int32_extended_atomics : enable\n" +
    "static int atomicAdd(__global int *_arr, int _index, int _delta){\n" +
    "   return atomic_add(&_arr[_index], _delta);\n" +
    "}\n" +
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, unsigned int heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "typedef struct This_s{\n" +
    "   __global void *heap;\n" +
    "   __global uint *free_index;\n" +
    "   int alloc_failed;\n" +
    "   unsigned int heap_size;\n" +
    "   } This;\n" +
    "static int org_apache_spark_rdd_cl_tests_ArrayAllocTest$$anon$1__apply$mcII$sp(This *this, int in){\n" +
    "\n" +
    "   return(\n" +
    "   {\n" +
    "         __global int * __alloc0 = (__global int *)alloc(this->heap, this->free_index, this->heap_size, sizeof(int) + (sizeof(int) * (5)), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0); } *((__global int *)__alloc0) = (5); __alloc0 = (__global int *)(((__global int *)__alloc0) + 1); \n" +
    "\n" +
    "       __global int* intArr = __alloc0;      __global double * __alloc1 = (__global double *)alloc(this->heap, this->free_index, this->heap_size, sizeof(int) + (sizeof(double) * (2)), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0); } *((__global int *)__alloc1) = (2); __alloc1 = (__global double *)(((__global int *)__alloc1) + 1); \n" +
    "\n" +
    "       __global double* doubleArr = __alloc1;\n" +
    "      (in + 3);\n" +
    "   });\n" +
    "}\n" +
    "static int org_apache_spark_rdd_cl_tests_ArrayAllocTest$$anon$1__apply(This *this, int in){\n" +
    "\n" +
    "   return(org_apache_spark_rdd_cl_tests_ArrayAllocTest$$anon$1__apply$mcII$sp(this, in));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global int* out, __global void *heap, __global uint *free_index, unsigned int heap_size, __global int *processing_succeeded, __global int *any_failed, int N) {\n" +
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
    "      out[i] = org_apache_spark_rdd_cl_tests_ArrayAllocTest$$anon$1__apply(this, in0[i]);\n" +
    "      if (this->alloc_failed) {\n" +
    "         processing_succeeded[i] = 0;\n" +
    "         *any_failed = 1;\n" +
    "      } else {\n" +
    "         processing_succeeded[i] = 1;\n" +
    "         \n" +
    "      }\n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        val intArr = new Array[Int](5)
        val doubleArr = new Array[Double](2)
        in + 3
      }
    }
  }
}
