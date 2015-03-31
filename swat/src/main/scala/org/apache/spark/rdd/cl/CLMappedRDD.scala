package org.apache.spark.rdd.cl

import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.universe._

import java.util.LinkedList

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.KernelWriter.WriterAndKernel
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter.DIRECTION

class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)
    extends RDD[U](prev) {
  var knowType : Boolean = false
  var entryPoint : Entrypoint = null
  var openCL : String = null
  var ctx : Long = -1L

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val N = 1024
    val acc : Array[T] = new Array[T](N)
    val output : Array[U] = new Array[U](N)

    System.setProperty("com.amd.aparapi.enable.NEW", "true");
    val classModel : ClassModel = ClassModel.createClassModel(f.getClass)
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor

    // 1 argument expected for maps
    val params : LinkedList[ScalaParameter] =
        CodeGenUtil.getParamObjsFromMethodDescriptor(descriptor, 1)
    params.add(CodeGenUtil.getReturnObjsFromMethodDescriptor(descriptor))

    // val entryPoint : Entrypoint = classModel.getEntrypoint("apply", descriptor,
    //     f, params);

    // val writerAndKernel : WriterAndKernel = KernelWriter.writeToString(
    //     entryPoint, params)
    // val openCL : String = writerAndKernel.kernel
    // val writer : KernelWriter = writerAndKernel.writer

    // val ctx : Long = OpenCLBridge.createContext(openCL,
    //     entryPoint.requiresDoublePragma, entryPoint.requiresHeap);

    val iter = new Iterator[U] {
      val nested = firstParent[T].iterator(split, context)

      var index = 0
      var nLoaded = 0

      def next() : U = {
        if (index >= nLoaded) {
          assert(nested.hasNext)

          index = 0
          nLoaded = 0
          while (nLoaded < N && nested.hasNext) {
            acc(nLoaded) = nested.next
            nLoaded = nLoaded + 1
          }

          if (nLoaded > 0 && !knowType) {
            if (acc(0).isInstanceOf[Tuple2[_, _]]) {
              System.err.println("Input is a tuple2");
              val inputClassType1 = acc(0).asInstanceOf[Tuple2[_, _]]._1.getClass
              val inputClassType2 = acc(0).asInstanceOf[Tuple2[_, _]]._2.getClass

              val inputClassType1Name = CodeGenUtil.cleanClassName(
                  inputClassType1.getName)
              val inputClassType2Name = CodeGenUtil.cleanClassName(
                  inputClassType2.getName)

              val tuple2ClassModel : Tuple2ClassModel = Tuple2ClassModel.create(
                  CodeGenUtil.getDescriptorForClassName(inputClassType1Name),
                  inputClassType1Name, 
                  CodeGenUtil.getDescriptorForClassName(inputClassType2Name),
                  inputClassType2Name)
              ClassModel.addClassModelFor(acc(0).getClass, tuple2ClassModel)

              params.get(0).addTypeParameter(inputClassType1Name)
              params.get(0).addTypeParameter(inputClassType2Name)

              knowType = true
            }
          }

          if (entryPoint == null) {
            entryPoint = classModel.getEntrypoint("apply", descriptor,
                f, params);

            val writerAndKernel = KernelWriter.writeToString(
                entryPoint, params)
            openCL = writerAndKernel.kernel

            ctx = OpenCLBridge.createContext(openCL,
                entryPoint.requiresDoublePragma, entryPoint.requiresHeap);
          }

          OpenCLBridgeWrapper.setArrayArg[T](ctx, 0, acc, entryPoint)
          OpenCLBridgeWrapper.setUnitializedArrayArg(ctx, 1, output.size,
              classTag[U].runtimeClass, entryPoint)

          var argnum : Int = 2
          val iter = entryPoint.getReferencedClassModelFields.iterator
          while (iter.hasNext) {
            val field = iter.next
            OpenCLBridge.setArgByNameAndType(ctx, argnum, f, field.getName,
                field.getDescriptor, entryPoint)
            argnum = argnum + 1
          }

          val heapArgStart : Int = argnum
          if (entryPoint.requiresHeap) {
            argnum = argnum + OpenCLBridge.createHeap(ctx, argnum, 100 * 1024L * 1024L, nLoaded)
          }   

          OpenCLBridge.setIntArg(ctx, argnum, nLoaded)

          if (entryPoint.requiresHeap) {
            val anyFailed : Array[Int] = new Array[Int](1)
            do {
              OpenCLBridge.run(ctx, nLoaded);
              OpenCLBridgeWrapper.fetchArrayArg(ctx, argnum - 1, anyFailed, entryPoint)
              OpenCLBridge.resetHeap(ctx, heapArgStart)
            } while (anyFailed(0) > 0)
          } else {
            OpenCLBridge.run(ctx, nLoaded);
          }

          OpenCLBridgeWrapper.fetchArgFromUnitializedArray(ctx, 1, output,
              entryPoint)
        }

        val curr = index
        index = index + 1
        output(curr)
      }

      def hasNext : Boolean = {
        (index < nLoaded || nested.hasNext)
      }
    }
    iter
  }

  override def map[V: ClassTag](f: U => V): RDD[V] = {
    new CLMappedRDD(this, sparkContext.clean(f))
  }
}
