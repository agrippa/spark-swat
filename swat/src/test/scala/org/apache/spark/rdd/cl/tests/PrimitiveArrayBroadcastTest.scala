package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

import org.apache.spark.broadcast.Broadcast

object PrimitiveArrayBroadcastTest extends CodeGenTest[Int, Int] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, unsigned int heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "typedef struct This_s{\n" +
    "   __global int* broadcast$1;\n" +
    "   } This;\n" +
    "static int org_apache_spark_rdd_cl_tests_PrimitiveArrayBroadcastTest$$anon$1__apply$mcII$sp(This *this, int in){\n" +
    "\n" +
    "   return(this->broadcast$1[in]);\n" +
    "}\n" +
    "static int org_apache_spark_rdd_cl_tests_PrimitiveArrayBroadcastTest$$anon$1__apply(This *this, int in){\n" +
    "\n" +
    "   return(org_apache_spark_rdd_cl_tests_PrimitiveArrayBroadcastTest$$anon$1__apply$mcII$sp(this, in));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global int* in0, \n" +
    "      __global int* out, __global int* broadcast$1, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   this->broadcast$1 = broadcast$1;\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_PrimitiveArrayBroadcastTest$$anon$1__apply(this, in0[i]);\n" +
    "      \n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    val broadcast : Broadcast[Array[Int]] = null

    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        broadcast.value(in)
      }
    }
  }
}
