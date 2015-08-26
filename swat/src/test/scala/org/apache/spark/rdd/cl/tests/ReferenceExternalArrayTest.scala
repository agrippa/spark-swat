package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

object ReferenceExternalArrayTest extends CodeGenTest[Int, Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, unsigned int heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "typedef struct This_s{\n" +
    "   __global int* arr$1;\n" +
    "   int arr$1__javaArrayLength;\n" +
    "   } This;\n" +
    "static int org_apache_spark_rdd_cl_tests_ReferenceExternalArrayTest$$anon$1__apply$mcII$sp(This *this, int in){\n" +
    "\n" +
    "   return((((in + this->arr$1[0]) + this->arr$1[1]) + this->arr$1[2]));\n" +
    "}\n" +
    "static int org_apache_spark_rdd_cl_tests_ReferenceExternalArrayTest$$anon$1__apply(This *this, int in){\n" +
    "\n" +
    "   return(org_apache_spark_rdd_cl_tests_ReferenceExternalArrayTest$$anon$1__apply$mcII$sp(this, in));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global int* out, __global int* arr$1, int arr$1__javaArrayLength, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   this->arr$1 = arr$1;\n" +
    "   this->arr$1__javaArrayLength = arr$1__javaArrayLength;\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_ReferenceExternalArrayTest$$anon$1__apply(this, in0[i]);\n" +
    "      \n" +
    "   }\n" +
    "}\n" +
    ""
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    val arr : Array[Int] = new Array[Int](3)
    arr(0) = 1
    arr(1) = 2
    arr(2) = 3

    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        in + arr(0) + arr(1) + arr(2)
      }
    }
  }
}
