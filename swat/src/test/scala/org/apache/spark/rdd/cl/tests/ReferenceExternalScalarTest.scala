package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter
import org.apache.spark.rdd.cl.CodeGenTest

object ReferenceExternalScalarTest extends CodeGenTest[Int, Int] {
  def getExpectedKernel() : String = {
"static __global void *alloc(__global void *heap, volatile __global uint *free_index, long heap_size, int nbytes, int *alloc_failed) {\n" +
"   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
"   uint offset = atomic_add(free_index, nbytes);\n" +
"   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
"   else return (__global void *)(cheap + offset);\n" +
"}\n" +
"typedef struct This_s{\n" +
"   int v$1;\n" +
"   } This;\n" +
"static int org_apache_spark_rdd_cl_tests_ReferenceExternalScalarTest$$anon$1__apply$mcII$sp(This *this, int in){\n" +
"   return((in + this->v$1));\n" +
"}\n" +
"static int org_apache_spark_rdd_cl_tests_ReferenceExternalScalarTest$$anon$1__apply(This *this, int in){\n" +
"   return(org_apache_spark_rdd_cl_tests_ReferenceExternalScalarTest$$anon$1__apply$mcII$sp(this, in));\n" +
"}\n" +
"__kernel void run(\n" +
"      __global int* in0, \n" +
"      __global int* out, int v$1, int N) {\n" +
"   int i = get_global_id(0);\n" +
"   int nthreads = get_global_size(0);\n" +
"   This thisStruct;\n" +
"   This* this=&thisStruct;\n" +
"   this->v$1 = v$1;\n" +
"   for (; i < N; i += nthreads) {\n" +
"      out[i] = org_apache_spark_rdd_cl_tests_ReferenceExternalScalarTest$$anon$1__apply(this, in0[i]);\n" +
"      \n" +
"   }\n" +
"}\n" +
""
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() { }

  def complete(params : LinkedList[ScalaParameter]) { }

  def getFunction() : Function1[Int, Int] = {
    val v : Int = 4

    new Function[Int, Int] {
      override def apply(in : Int) : Int = {
        in + v
      }
    }
  }
}
