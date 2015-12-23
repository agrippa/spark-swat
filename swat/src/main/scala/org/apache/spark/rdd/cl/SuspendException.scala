package org.apache.spark.rdd.cl

class SuspendException(message: String = null, cause : Throwable = null)
    extends RuntimeException(message, cause) {
}
