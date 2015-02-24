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

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    System.setProperty("com.amd.aparapi.enable.INVOKEINTERFACE", "true")

    val N = 1024
    val acc : Array[T] = new Array[T](N)
    val output : Array[U] = new Array[U](N)

    val classModel : ClassModel = ClassModel.createClassModel(f.getClass)
    val entryPoint : Entrypoint = classModel.getEntrypoint("apply", "(D)D", f); 

    val params = new LinkedList[ScalaParameter]()
    params.add(new ScalaParameter("double*", "v", DIRECTION.IN))
    params.add(new ScalaParameter("double*", "out", DIRECTION.OUT))

    val openCL : String = KernelWriter.writeToString(entryPoint, params)
    System.err.println("OPENCL:\n" + openCL);
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

          for (i <- 0 until nLoaded) {
            output(i) = f(acc(i))
          }
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
