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

object EntrypointCache {
  val cache : java.util.Map[EntrypointCacheKey, Entrypoint] =
      new java.util.HashMap[EntrypointCacheKey, Entrypoint]()
  val kernelCache : java.util.Map[EntrypointCacheKey, java.lang.String] =
      new java.util.HashMap[EntrypointCacheKey, java.lang.String]()
}

class EntrypointCacheKey(className : java.lang.String)
    extends java.lang.Comparable[EntrypointCacheKey] {
  def getClassName() : java.lang.String = { className }

  def compareTo(other : EntrypointCacheKey) : Int = {
    return className.compareTo(other.getClassName)
  }

  override def equals(otherObj : Any) : Boolean = {
    if (otherObj.isInstanceOf[EntrypointCacheKey]) {
      compareTo(otherObj.asInstanceOf[EntrypointCacheKey]) == 0
    } else {
      false
    }
  }

  override def hashCode() : Int = {
    className.hashCode
  }
}
