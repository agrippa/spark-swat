package org.apache.spark.rdd.cl

import java.nio.ByteBuffer
import java.nio.ByteOrder

class ByteBufferCache(val capacity : Int) {
  val buffers : Array[Option[ByteBuffer]] = new Array[Option[ByteBuffer]](capacity)

  for (i <- 0 until capacity) {
    buffers(i) = Some(ByteBuffer.allocate(1024 * 1024))
    buffers(i).get.order(ByteOrder.LITTLE_ENDIAN)
  }

  def roundUpLog2(v : Int) : Int = {
    scala.math.pow(2, scala.math.ceil(scala.math.log(v)/scala.math.log(2))).asInstanceOf[Int]
  }

  def releaseBuffer(bb : ByteBuffer) {
    for (i <- 0 until capacity) {
      if (buffers(i).isEmpty) {
        buffers(i) = Some(bb)
        return
      }
    }
    throw new RuntimeException()
  }

  def getBuffer(length : Int) : ByteBuffer = {
    var bestMatch : Int = -1
    var smallest : Int = -1

    for (i <- 0 until capacity) {
      if (!buffers(i).isEmpty) {
        if (smallest == -1 || buffers(i).get.capacity <
            buffers(smallest).get.capacity) {
          smallest = i
        }

        if (buffers(i).get.capacity >= length) {
          if (bestMatch == -1 || buffers(bestMatch).get.capacity >
              buffers(i).get.capacity) {
            bestMatch = i
          }
        }
      }
    }

    if (bestMatch != -1) {
      val result : ByteBuffer = buffers(bestMatch).get
      buffers(bestMatch) = None
      result.clear
      return result
    } else {
      assert(smallest != -1)
      buffers(smallest) = None
      val newBufferSize : Int = roundUpLog2(length)
      val newBuffer : ByteBuffer = ByteBuffer.allocate(newBufferSize)
      newBuffer.order(ByteOrder.LITTLE_ENDIAN)
      return newBuffer
    }
  }
}
