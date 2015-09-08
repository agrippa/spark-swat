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
}
