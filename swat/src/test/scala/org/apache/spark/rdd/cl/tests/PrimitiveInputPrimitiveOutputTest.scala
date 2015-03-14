package org.apache.spark.rdd.cl.tests

import org.apache.spark.rdd.cl.CodeGenTest

class PrimitiveInputPrimitiveOutputTest extends CodeGenTest {
  def getExpectedKernel() : String = {
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, long heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "typedef struct This_s{\n" +
    "   }This;\n" +
    "static int org_apache_spark_rdd_cl_tests_PrimitiveInputPrimitiveOutputTest__apply(This *this, int in){\n" +
    "   return((in + 3));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global int* out, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_PrimitiveInputPrimitiveOutputTest__apply(this, in0[i]);\n" +
    "      \n" +
    "   }\n" +
    "}\n" +
    ""
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def apply(in : Int) : Int = {
    in + 3
  }
}
