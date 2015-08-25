package org.apache.spark.rdd.cl.tests

import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import org.apache.spark.rdd.cl.CodeGenTest
import com.amd.aparapi.internal.model.HardCodedClassModels

object ReferenceExternalObjectArrayTest extends CodeGenTest[Float, Float] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = {
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
    "   __global org_apache_spark_rdd_cl_tests_Point* arr$1;\n" +
    "   int arr$1__javaArrayLength;\n" +
    "   } This;\n" +
    "static float org_apache_spark_rdd_cl_tests_Point__z(__global org_apache_spark_rdd_cl_tests_Point *this){\n" +
    "   return this->z;\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_Point__y(__global org_apache_spark_rdd_cl_tests_Point *this){\n" +
    "   return this->y;\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_Point__x(__global org_apache_spark_rdd_cl_tests_Point *this){\n" +
    "   return this->x;\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_ReferenceExternalObjectArrayTest$$anon$1__apply$mcFF$sp(This *this, float in){\n" +
    "   return((((in +  (this->arr$1[0]).x) +  (this->arr$1[1]).y) +  (this->arr$1[2]).z));\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_ReferenceExternalObjectArrayTest$$anon$1__apply(This *this, float in){\n" +
    "   return(org_apache_spark_rdd_cl_tests_ReferenceExternalObjectArrayTest$$anon$1__apply$mcFF$sp(this, in));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global float* in0, \n" +
    "      __global float* out, __global org_apache_spark_rdd_cl_tests_Point* arr$1, int arr$1__javaArrayLength, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   this->arr$1 = arr$1;\n" +
    "   this->arr$1__javaArrayLength = arr$1__javaArrayLength;\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_ReferenceExternalObjectArrayTest$$anon$1__apply(this, in0[i]);\n" +
    "      \n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Float, Float] = {
    val arr : Array[Point] = new Array[Point](3)
    arr(0) = new Point(1.0f, 2.0f, 3.0f)
    arr(1) = new Point(4.0f, 5.0f, 6.0f)
    arr(2) = new Point(7.0f, 8.0f, 9.0f)

    new Function[Float, Float] {
      override def apply(in : Float) : Float = {
        in + arr(0).x + arr(1).y + arr(2).z
      }
    }
  }
}
