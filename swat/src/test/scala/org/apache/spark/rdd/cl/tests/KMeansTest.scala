package org.apache.spark.rdd.cl.tests

import scala.math._
import java.util.LinkedList
import com.amd.aparapi.internal.writer.BlockWriter.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenUtil
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels

class PointWithClassifier(val x: Float, val y: Float, val z: Float)
    extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f)
  }

  def dist(center : PointWithClassifier) : (Float) = {
    val diffx : Float = center.x - x
    val diffy : Float = center.y - y
    val diffz : Float = center.z - z
    sqrt(diffx * diffx + diffy * diffy + diffz * diffz).asInstanceOf[Float]
  }
}
object KMeansTest extends CodeGenTest[PointWithClassifier, (Int, PointWithClassifier)] {
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
    "typedef struct __attribute__ ((packed)) org_apache_spark_rdd_cl_tests_PointWithClassifier_s{\n" +
    "   float  z;\n" +
    "   float  y;\n" +
    "   float  x;\n" +
    "   \n" +
    "} org_apache_spark_rdd_cl_tests_PointWithClassifier;\n" +
    "\n" +
    "typedef struct __attribute__ ((packed)) scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier_s{\n" +
    "   __global org_apache_spark_rdd_cl_tests_PointWithClassifier  * _2;\n" +
    "   int  _1;\n" +
    "   \n" +
    "} scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier;\n" +
    "typedef struct This_s{\n" +
    "   __global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier *centers$1;\n" +
    "   int centers$1__javaArrayLength;\n" +
    "   __global void *heap;\n" +
    "   __global uint *free_index;\n" +
    "   int alloc_failed;\n" +
    "   unsigned int heap_size;\n" +
    "   } This;\n" +
    "\n" +
    "static __global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier *scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier___init_(__global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier *this, int  one, __global org_apache_spark_rdd_cl_tests_PointWithClassifier *  two) {\n" +
    "   this->_1 = one;\n" +
    "   this->_2 = two;\n" +
    "   return this;\n" +
    "}\n" +
    "\n" +
    "static float org_apache_spark_rdd_cl_tests_PointWithClassifier__z(__global org_apache_spark_rdd_cl_tests_PointWithClassifier *this){\n" +
    "   return this->z;\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_PointWithClassifier__y(__global org_apache_spark_rdd_cl_tests_PointWithClassifier *this){\n" +
    "   return this->y;\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_PointWithClassifier__x(__global org_apache_spark_rdd_cl_tests_PointWithClassifier *this){\n" +
    "   return this->x;\n" +
    "}\n" +
    "static __global org_apache_spark_rdd_cl_tests_PointWithClassifier * org_apache_spark_rdd_cl_tests_PointWithClassifier___init_(__global org_apache_spark_rdd_cl_tests_PointWithClassifier *this, float x, float y, float z){\n" +
    "   this->x=x;\n" +
    "   this->y=y;\n" +
    "   this->z=z;\n" +
    "   (this);\n" +
    "   return (this);\n" +
    "}\n" +
    "static float org_apache_spark_rdd_cl_tests_PointWithClassifier__dist(__global org_apache_spark_rdd_cl_tests_PointWithClassifier *this, __global org_apache_spark_rdd_cl_tests_PointWithClassifier* center){\n" +
    "   return(\n" +
    "   {\n" +
    "      float diffx = center->x - this->x;\n" +
    "      float diffy = center->y - this->y;\n" +
    "      float diffz = center->z - this->z;\n" +
    "      (float)sqrt((double)(((diffx * diffx) + (diffy * diffy)) + (diffz * diffz)));\n" +
    "   });\n" +
    "}\n" +
    "static __global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier *org_apache_spark_rdd_cl_tests_KMeansTest$$anon$1__apply(This *this, __global org_apache_spark_rdd_cl_tests_PointWithClassifier* in){\n" +
    "   return(\n" +
    "   {\n" +
    "      int closest_center = -1;\n" +
    "      float closest_center_dist = -1.0f;\n" +
    "      int i = 0;\n" +
    "      while (i<this->centers$1__javaArrayLength){\n" +
    "         {\n" +
    "            float d = org_apache_spark_rdd_cl_tests_PointWithClassifier__dist(in,  (this->centers$1[i])._2);\n" +
    "            if (i==0 || d<closest_center_dist){\n" +
    "               closest_center = i;\n" +
    "               closest_center_dist = d;\n" +
    "            }\n" +
    "            i = i + 1;\n" +
    "         }\n" +
    "      }\n" +
    "      ;\n" +
    "      __global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier * __alloc0 = (__global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier *)alloc(this->heap, this->free_index, this->heap_size, sizeof(scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); }\n" +
    "      __global org_apache_spark_rdd_cl_tests_PointWithClassifier * __alloc1 = (__global org_apache_spark_rdd_cl_tests_PointWithClassifier *)alloc(this->heap, this->free_index, this->heap_size, sizeof(org_apache_spark_rdd_cl_tests_PointWithClassifier), &this->alloc_failed);\n" +
    "      if (this->alloc_failed) { return (0x0); }\n" +
    "      scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier___init_(__alloc0,  (this->centers$1[closest_center])._1, org_apache_spark_rdd_cl_tests_PointWithClassifier___init_(__alloc1,  (this->centers$1[closest_center])._2->x,  (this->centers$1[closest_center])._2->y,  (this->centers$1[closest_center])._2->z));\n" +
    "   });\n" +
    "}\n" +
    "__kernel void run(\n" +
    "      __global org_apache_spark_rdd_cl_tests_PointWithClassifier* in0, \n" +
    "      __global int * out_1, __global org_apache_spark_rdd_cl_tests_PointWithClassifier* out_2, __global int * centers$1_1, __global org_apache_spark_rdd_cl_tests_PointWithClassifier* centers$1_2, __global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier *centers$1, int centers$1__javaArrayLength, __global void *heap, __global uint *free_index, unsigned int heap_size, __global int *processing_succeeded, __global int *any_failed, int N) {\n" +
    "   int i = get_global_id(0);\n" +
    "   int nthreads = get_global_size(0);\n" +
    "   This thisStruct;\n" +
    "   This* this=&thisStruct;\n" +
    "   this->centers$1 = centers$1; for (int i = 0; i < centers$1__javaArrayLength; i++) { centers$1[i]._1 = centers$1_1[i]; centers$1[i]._2 = centers$1_2 + i;  } ;\n" +
    "   this->centers$1__javaArrayLength = centers$1__javaArrayLength;\n" +
    "   this->heap = heap;\n" +
    "   this->free_index = free_index;\n" +
    "   this->heap_size = heap_size;\n" +
    "   for (; i < N; i += nthreads) {\n" +
    "      if (processing_succeeded[i]) continue;\n" +
    "      \n" +
    "      this->alloc_failed = 0;\n" +
    "      __global scala_Tuple2_I_org_apache_spark_rdd_cl_tests_PointWithClassifier* result = org_apache_spark_rdd_cl_tests_KMeansTest$$anon$1__apply(this, in0 + i);\n" +
    "      if (this->alloc_failed) {\n" +
    "         processing_succeeded[i] = 0;\n" +
    "         *any_failed = 1;\n" +
    "      } else {\n" +
    "         processing_succeeded[i] = 1;\n" +
    "         out_1[i] = result->_1;\n" +
    "         out_2[i] = *(result->_2);\n" +
    "      }\n" +
    "   }\n" +
    "}\n"
  }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val outputClassType1Name = CodeGenUtil.cleanClassName("I")
    val outputClassType2Name = CodeGenUtil.cleanClassName(
        "org.apache.spark.rdd.cl.tests.PointWithClassifier")

    val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
        outputClassType1Name, outputClassType2Name, true)
    val models = new HardCodedClassModels()
    models.addClassModelFor(classOf[Tuple2[_, _]], tuple2ClassModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
    params.get(1).addTypeParameter("I", false)
    params.get(1).addTypeParameter(
        "Lorg.apache.spark.rdd.cl.tests.PointWithClassifier;", true)
  }

  def getFunction() : Function1[PointWithClassifier, (Int, PointWithClassifier)] = {
    var centers = new Array[(Int, PointWithClassifier)](3)
    for (i <- 0 until 3) {
        centers(i) = (i, new PointWithClassifier(i, 2.0f * i, 3.0f * i))
    }
    new Function[PointWithClassifier, (Int, PointWithClassifier)] {
      override def apply(in : PointWithClassifier) : (Int, PointWithClassifier) = {
        var closest_center = -1
        var closest_center_dist = -1.0f

        var i = 0
        while (i < centers.length) {
          val d = in.dist(centers(i)._2)
          if (i == 0 || d < closest_center_dist) {
            closest_center = i
            closest_center_dist = d
          }

          i += 1
        }
        (centers(closest_center)._1, new PointWithClassifier(
            centers(closest_center)._2.x, centers(closest_center)._2.y,
            centers(closest_center)._2.z))
      }
    }
  }
}
