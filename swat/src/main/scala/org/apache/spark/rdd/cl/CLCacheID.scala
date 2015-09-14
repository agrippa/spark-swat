package org.apache.spark.rdd.cl

/*
 * Represents a uniquely identified object that may be cached by the GPU memory
 * manager, keyed on this ID. This is only used for broadcast variables or for
 * input RDD partitions that have been explicitly cached by the programmer using
 * the Spark "cache" operation.
 */
class CLCacheID(val broadcast : Int, val rdd : Int, val partition : Int,
    val offset : Int, var component : Int) {
  def this(broadcast : Int, component : Int) = this(broadcast, -1, -1, -1, component)
  def this(rdd : Int, partition : Int, offset : Int, component : Int) =
      this(-1, rdd, partition, offset, component)

  def incrComponent(delta : Int) {
    component += delta
  }
}

object NoCache extends CLCacheID(-1, -1, -1, -1, -1) {
}
