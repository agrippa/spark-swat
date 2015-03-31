package org.apache.spark.rdd.cl.tests

import org.apache.spark.rdd.cl.CodeGenTest

object PrimitiveInputObjectOutputTest extends CodeGenTest[Int, Point] {
  def getExpectedKernel() : String = {
    "#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable\n" +
    "#pragma OPENCL EXTENSION cl_khr_int64_extended_atomics : enable\n" +
    "static int atomicAdd(__global int *_arr, int _index, int _delta){\n" +
    "   return atomic_add(&_arr[_index], _delta);\n" +
    "}\n" +
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, long heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "\n" +
    "typedef struct org_apache_spark_rdd_cl_tests_Point_s{\n" +
    "   float  x;\n" +
    "   float  y;\n" +
    "   float  z;\n" +
    "   \n" +
    "} org_apache_spark_rdd_cl_tests_Point;\n" +
    "typedef struct This_s{\n" +
    "   __global void *heap;\n" +
    "   __global uint *free_index;\n" +
    "   int alloc_failed;\n" +
    "   long heap_size;\n" +
    "   } This;\n" +
    "static __global org_apache_spark_rdd_cl_tests_Point * org_apache_spark_rdd_cl_tests_Point___init_(__global org_apache_spark_rdd_cl_tests_Point *this, float x, float y, float z){\n" +
    "   this->x=x;\n" +
    "   this->y=y;\n" +
    "   this->z=z;\n" +
    "   (this);\n" +
    "   return (this);\n" +
    "}\n" +
    "static __global org_apache_spark_rdd_cl_tests_Point *org_apache_spark_rdd_cl_tests_PrimitiveInputObjectOutputTest$$anon$1__apply(This *this, int in){\n" +
    "   __global org_apache_spark_rdd_cl_tests_Point * __alloc0 = (__global org_apache_spark_rdd_cl_tests_Point *)alloc(this->heap, this->free_index, this->heap_size, sizeof(org_apache_spark_rdd_cl_tests_Point), &this->alloc_failed);\n" +
    "   if (this->alloc_failed) { return (0x0); }\n" +
    "   return(org_apache_spark_rdd_cl_tests_Point___init_(__alloc0, (float)(in + 1), (float)(in + 2), (float)(in + 3)));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global org_apache_spark_rdd_cl_tests_Point* out, __global void *heap, __global uint *free_index, long heap_size, __global int *processing_succeeded, __global int *any_failed, int N) {\n" +
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
    "      __global org_apache_spark_rdd_cl_tests_Point* result = org_apache_spark_rdd_cl_tests_PrimitiveInputObjectOutputTest$$anon$1__apply(this, in0[i]);\n" +
    "      if (this->alloc_failed) {\n" +
    "         processing_succeeded[i] = 0;\n" +
    "         *any_failed = 1;\n" +
    "      } else {\n" +
    "         processing_succeeded[i] = 1;\n" +
    "         out[i] = *result;\n" +
    "      }\n" +
    "   }\n" +
    "}\n" +
    "";
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def getFunction() : Function1[Int, Point] = {
    new Function[Int, Point] {
      override def apply(in : Int) : Point = {
        new Point(in + 1, in + 2, in + 3)
      }
    }
  }
}
