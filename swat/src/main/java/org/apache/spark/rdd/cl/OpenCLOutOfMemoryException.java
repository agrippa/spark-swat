package org.apache.spark.rdd.cl;

public class OpenCLOutOfMemoryException extends Exception {
    public OpenCLOutOfMemoryException(int len) {
        super("len=" + len);
    }

    public OpenCLOutOfMemoryException() { }

    public OpenCLOutOfMemoryException(String msg) { super(msg); }
}
