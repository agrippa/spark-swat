package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

object ObjectInputObjectOutputTest extends CodeGenTest[Point, Point] {
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
    "\n" +
    "typedef struct __attribute__ ((packed)) org_apache_spark_rdd_cl_tests_Point_s{\n" +
    "   float  x;\n" +
    "   float  y;\n" +
    "   float  z;\n" +
    "   \n" +
    "} org_apache_spark_rdd_cl_tests_Point;\n" +
    "typedef struct This_s{\n" +
    "   __global void *heap;\n" +
    "   __global uint *free_index;\n" +
    "   int alloc_failed;\n" +
    "   unsigned int heap_size;\n" +
    "   } This;\n" +
    "static __global org_apache_spark_rdd_cl_tests_Point * org_apache_spark_rdd_cl_tests_Point___init_(__global org_apache_spark_rdd_cl_tests_Point *this, float x, float y, float z){\n" +
    "\n" +
    "   this->x=x;\n" +
    "   this->y=y;\n" +
    "   this->z=z;\n" +
    "   (this);\n" +
    "   return (this);\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_Point__z(__global org_apache_spark_rdd_cl_tests_Point *this){\n" +
    "   return this->z;\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_Point__y(__global org_apache_spark_rdd_cl_tests_Point *this){\n" +
    "   return this->y;\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_Point__x(__global org_apache_spark_rdd_cl_tests_Point *this){\n" +
    "   return this->x;\n" +
    "}\n" +
    "static __global org_apache_spark_rdd_cl_tests_Point *org_apache_spark_rdd_cl_tests_ObjectInputObjectOutputTest$$anon$1__apply(This *this, __global org_apache_spark_rdd_cl_tests_Point* in){\n" +
    "\n" +
    "   __global org_apache_spark_rdd_cl_tests_Point * __alloc0 = (__global org_apache_spark_rdd_cl_tests_Point *)alloc(this->heap, this->free_index, this->heap_size, sizeof(org_apache_spark_rdd_cl_tests_Point), &this->alloc_failed);\n" +
    "   if (this->alloc_failed) { return (0x0); }\n" +
    "   return(org_apache_spark_rdd_cl_tests_Point___init_(__alloc0, (in->x + (float)1), (in->y + (float)2), (in->z + (float)3)));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global org_apache_spark_rdd_cl_tests_Point* in0, \n" +
    "      __global org_apache_spark_rdd_cl_tests_Point* out, __global void *heap, __global uint *free_index, unsigned int heap_size, __global int *processing_succeeded, __global int *any_failed, int N) {\n" +
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
    "      __global org_apache_spark_rdd_cl_tests_Point* result = org_apache_spark_rdd_cl_tests_ObjectInputObjectOutputTest$$anon$1__apply(this, in0 + i);\n" +
    "      if (this->alloc_failed) {\n" +
    "         processing_succeeded[i] = 0;\n" +
    "         *any_failed = 1;\n" +
    "      } else {\n" +
    "         processing_succeeded[i] = 1;\n" +
    "         out[i] = *result;\n" +
    "      }\n" +
    "   }\n" +
    "}\n" +
    ""
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Point, Point] = {
    new Function[Point, Point] {
      override def apply(in : Point) : Point = {
        new Point(in.x + 1, in.y + 2, in.z + 3)
      }
    }
  }
}
