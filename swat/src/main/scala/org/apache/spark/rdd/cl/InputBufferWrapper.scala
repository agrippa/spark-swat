/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.apache.spark.rdd.cl

import com.amd.aparapi.internal.model.Entrypoint

/*
 * The interface presetnted by all input buffers. Input buffers aggregate input
 * items from the input stream of a particular partition and prepare them for
 * one of two modes of execution. You can access an input buffer through its
 * next/hasNext API, using it just like an iterator. This case is used when an
 * OOM error during kernel setup prevents OpenCL execution, forcing us to revert
 * to JVM execution. An input buffer can also be used to copy to an OpenCL
 * device in preparation for launching a parallel kernel on its contents.
 */
trait InputBufferWrapper[T] {

  // Add a single object to the input buffer
  def append(obj : Any)
  /*
   * Add many objects from the provided iterator, may use append behind the
   * scenes. The number of objects added is limited by the number of objects
   * accessible through iter and the available space to store them in this input
   * buffer.
   */
  def aggregateFrom(iter : Iterator[T])

  // Ensure as many stored items as possible are serialized
  def flush()

  /*
   * Used to check if an input buffer has read items from the parent partition
   * but not copied them to the device yet. For many buffer types this will
   * always return false. For dense vector or sparse vector buffers, may return
   * true if they read a vector but do not have space in their byte buffers to
   * store it.
   */
  def haveUnprocessedInputs : Boolean

  /*
   * Returns true when an input buffer has been filled to the point where it can
   * accept no more elements.
   */
  def outOfSpace : Boolean

  /*
   * Return the number of kernel arguments this type of input buffer will use.
   */
  def countArgumentsUsed : Int

  def nBuffered() : Int
  def reset()

  // Returns # of arguments used
  def tryCache(id : CLCacheID, ctx : Long, dev_ctx : Long,
      entryPoint : Entrypoint, persistent : Boolean) : Int

  def selfAllocate(dev_ctx : Long)
  def generateNativeInputBuffer(dev_ctx : Long) : NativeInputBuffers[T]
  def getCurrentNativeBuffers() : NativeInputBuffers[T]
  def setCurrentNativeBuffers(set : NativeInputBuffers[_])

  // Must be called prior to transferOverflowTo
  def setupNativeBuffersForCopy(limit : Int)
  def transferOverflowTo(otherAbstract : NativeInputBuffers[_]) :
      NativeInputBuffers[T]
}
