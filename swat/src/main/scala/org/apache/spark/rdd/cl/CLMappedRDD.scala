package org.apache.spark.rdd.cl

import scala.reflect.ClassTag

import java.util.LinkedList

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd._

import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.Entrypoint
import com.amd.aparapi.internal.writer.KernelWriter
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter
import com.amd.aparapi.internal.writer.BlockWriter.ScalaParameter.DIRECTION

class CLMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)
  extends RDD[U](prev) {

  def isPrimitive(typeString : String) : Boolean = { 
    return typeString.equals("I") || typeString.equals("D") || typeString.equals("F")
  }

  def getPrimitiveTypeForDescriptor(descString : String) : String = { 
    assert(isPrimitive(descString))
    if (descString.equals("I")) {
      return "int"
    } else if (descString.equals("D")) {
      return "double"
    } else if (descString.equals("F")) {
      return "float"
    } else {
      throw new RuntimeException("Unsupported type")
    }   
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    val N = 1024
    val acc : Array[T] = new Array[T](N)
    val output : Array[U] = new Array[U](N)

    val classModel : ClassModel = ClassModel.createClassModel(f.getClass)
    val method = classModel.getPrimitiveApplyMethod
    val descriptor : String = method.getDescriptor
    val returnType : String = descriptor.substring(descriptor.lastIndexOf(')') + 1)
    val arguments : String = descriptor.substring(descriptor.indexOf('(') + 1, descriptor.lastIndexOf(')'))
    val argumentsArr : Array[String] = arguments.split(",")
    assert(isPrimitive(returnType))
    for (arg <- argumentsArr) {
      assert(isPrimitive(arg))
    }
    assert(argumentsArr.length == 1) // For map

    val entryPoint : Entrypoint = classModel.getEntrypoint("apply", descriptor, f);

    val params = new LinkedList[ScalaParameter]()
    params.add(new ScalaParameter(getPrimitiveTypeForDescriptor(argumentsArr(0)) + "*", "in", DIRECTION.IN))
    params.add(new ScalaParameter(getPrimitiveTypeForDescriptor(returnType) + "*", "out", DIRECTION.OUT))

    val openCL : String = KernelWriter.writeToString(entryPoint, params)
    val ctx : Long = OpenCLBridge.createContext(openCL);

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

          OpenCLBridgeWrapper.setArrayArg(ctx, 0, acc)
          OpenCLBridgeWrapper.setArrayArg(ctx, 1, output)
          OpenCLBridge.setIntArg(ctx, 2, nLoaded)

          OpenCLBridge.run(ctx, nLoaded);

          OpenCLBridgeWrapper.fetchArrayArg(ctx, 1, output);
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
}
