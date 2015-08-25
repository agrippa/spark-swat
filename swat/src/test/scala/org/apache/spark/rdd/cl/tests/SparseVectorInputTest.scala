package org.apache.spark.rdd.cl.tests 
import java.util.LinkedList

import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.SparseVectorClassModel

import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil

import org.apache.spark.mllib.linalg.SparseVector

import org.apache.spark.rdd.cl.SparseVectorInputBufferWrapperConfig

object SparseVectorInputTest extends CodeGenTest[SparseVector, (Int, Double)] {
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
    "   uint offset = atomic_add(free_index, nbytes);\n" +
    "   if (offset + nbytes > heap_size) { *alloc_failed = 1; return 0x0; }\n" +
    "   else return (__global void *)(cheap + offset);\n" +
    "}\n" +
    "\n" +
    "typedef struct __attribute__ ((packed)) org_apache_spark_mllib_linalg_SparseVector_s{\n" +
    "   __global double*  values;\n" +
    "   __global int*  indices;\n" +
    "   int  size;\n" +
    "   \n" +
    "} org_apache_spark_mllib_linalg_SparseVector;\n" +
    "\n" +
    "typedef struct __attribute__ ((packed)) scala_Tuple2_I_D_s{\n" +
    "   double  _2;\n" +
    "   int  _1;\n" +
    "   \n" +
    "} scala_Tuple2_I_D;\n" +
    "typedef struct This_s{\n" +
    "   __global void *heap;\n" +
    "   __global uint *free_index;\n" +
    "   int alloc_failed;\n" +
    "   unsigned int heap_size;\n" +
    "   } This;\n" +
    "\n" +
    "static __global scala_Tuple2_I_D *scala_Tuple2_I_D___init_(__global scala_Tuple2_I_D *this, int  one, double  two) {\n" +
    "   this->_1 = one;\n" +
    "   this->_2 = two;\n" +
    "   return this;\n" +
    "}\n" +
    "\n" +
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
    "static __global scala_Tuple2_I_D *org_apache_spark_rdd_cl_tests_SparseVectorInputTest$$anon$1__apply(This *this, __global org_apache_spark_mllib_linalg_SparseVector* in){\n" +
    "   return(\n" +
    "   {\n" +
    "      int indexSum = 0;\n" +
    "      double valueSum = 0.0;\n" +
    "      int i = 0;\n" +
    "      for (; i<org_apache_spark_mllib_linalg_SparseVector__size(in); i = i + 1){\n" +
    "         indexSum = indexSum + org_apache_spark_mllib_linalg_SparseVector__indices(in)[32 * (i)];\n" +
    "         valueSum = valueSum + org_apache_spark_mllib_linalg_SparseVector__values(in)[32 * (i)];\n" +
    "      }\n" +
    "      ;\n" +
    "      __global scala_Tuple2_I_D * __alloc0 = (__global scala_Tuple2_I_D *)alloc(this->heap, this->free_index, this->heap_size, sizeof(scala_Tuple2_I_D), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); }\n" +
    "      scala_Tuple2_I_D___init_(__alloc0, indexSum, valueSum);\n" +
    "   });\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global org_apache_spark_mllib_linalg_SparseVector* in0, __global int *in0_indices, __global double *in0_values, __global int *in0_sizes, __global int *in0_offsets, \n" +
    "      __global int * out_1, __global double * out_2, __global void *heap, __global uint *free_index, unsigned int heap_size, __global int *processing_succeeded, __global int *any_failed, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   this->heap = heap;\n" +
    "   this->free_index = free_index;\n" +
    "   this->heap_size = heap_size;\n" +
    "   __global org_apache_spark_mllib_linalg_SparseVector *my_in0 = in0 + get_global_id(0);\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      if (processing_succeeded[i]) continue;\n" +
    "      \n" +
    "      this->alloc_failed = 0;\n" +
    "      my_in0->values = in0_values + in0_offsets[i];\n" +
    "      my_in0->indices = in0_indices + in0_offsets[i];\n" +
    "      my_in0->size = in0_sizes[i];\n" +
    "      __global scala_Tuple2_I_D* result = org_apache_spark_rdd_cl_tests_SparseVectorInputTest$$anon$1__apply(this, in0 + i);\n" +
    "      if (this->alloc_failed) {\n" +
    "         processing_succeeded[i] = 0;\n" +
    "         *any_failed = 1;\n" +
    "      } else {\n" +
    "         processing_succeeded[i] = 1;\n" +
    "         out_1[i] = result->_1;\n" +
    "         out_2[i] = result->_2;\n" +
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

    val outputClassType1Name = CodeGenUtil.cleanClassName("I")
    val outputClassType2Name = CodeGenUtil.cleanClassName("D")
    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        outputClassType1Name, outputClassType2Name, true)
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)

    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(1).addTypeParameter("I", false)
    params.get(1).addTypeParameter("D", false)
  }

  def getFunction() : Function1[SparseVector, (Int, Double)] = {
    new Function[SparseVector, (Int, Double)] {
      override def apply(in : SparseVector) : (Int, Double) = {
        var indexSum = 0
        var valueSum = 0.0
        var i = 0
        while (i < in.size) {
            indexSum += in.indices(i)
            valueSum += in.values(i)
            i += 1
        }
        (indexSum, valueSum)
      }
    }
  }
}
