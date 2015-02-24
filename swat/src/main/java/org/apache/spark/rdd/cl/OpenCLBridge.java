package org.apache.spark.rdd.cl;

public class OpenCLBridge {
  public static native long createContext(String _source);
}
