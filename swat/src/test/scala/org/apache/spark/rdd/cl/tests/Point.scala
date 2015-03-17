package org.apache.spark.rdd.cl.tests

class Point(var x: Float, var y: Float, var z: Float) {
  def update_values(inc : Int) {
    x = x + inc
    y = y + inc
    z = z + inc
  }
}
