package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel

import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil

object Tuple2InputTest extends CodeGenTest[(Int, Int), Int] {
  def getExpectedKernel() : String = {
    "static __global void *alloc(__global void *heap, volatile __global uint *free_index, long heap_size, int nbytes, int *alloc_failed) {\n" +
    "   __global unsigned char *cheap = (__global unsigned char *)heap;\n" +
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "\n" +
    "typedef struct scala_Tuple2_I_I_s{\n" +
    "   int  _1;\n" +
    "   int  _2;\n" +
    "   \n" +
    "} scala_Tuple2_I_I;\n" +
    "typedef struct This_s{\n" +
    "   } This;\n" +
    "static int org_apache_spark_rdd_cl_tests_Tuple2InputTest$$anon$1__apply(This *this, __global scala_Tuple2_I_I* in){\n" +
    "   return((in->_1 + in->_2));\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global scala_Tuple2_I_I* in0, \n" +
    "      __global int* out, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      out[i] = org_apache_spark_rdd_cl_tests_Tuple2InputTest$$anon$1__apply(this, in0 + i);\n" +
    "      \n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() {
    val inputClassType1Name = CodeGenUtil.cleanClassName("I")
    val inputClassType2Name = CodeGenUtil.cleanClassName("I")

    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        CodeGenUtil.getDescriptorForClassName(inputClassType1Name), inputClassType1Name, 
        CodeGenUtil.getDescriptorForClassName(inputClassType2Name), inputClassType2Name)
    ClassModel.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)
  }

  def complete(params : LinkedList[ScalaParameter]) {
    params.get(0).addTypeParameter("I")
    params.get(0).addTypeParameter("I")
  }

  def getFunction() : Function1[(Int, Int), Int] = {
    new Function[(Int, Int), Int] {
      override def apply(in : (Int, Int)) : Int = {
        in._1 + in._2
      }
    }
  }
}
