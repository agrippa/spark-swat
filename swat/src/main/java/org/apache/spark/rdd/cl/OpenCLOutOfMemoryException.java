package org.apache.spark.rdd.cl;

public class OpenCLOutOfMemoryException extends Exception {
    public OpenCLOutOfMemoryException() { }

    public OpenCLOutOfMemoryException(String msg) { super(msg); }
}
