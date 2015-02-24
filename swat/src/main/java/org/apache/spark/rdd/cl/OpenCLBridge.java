package org.apache.spark.rdd.cl;

public class OpenCLBridge {
    static {
        String swatHome = System.getenv("SWAT_HOME");
        assert(swatHome != null);
        System.load(swatHome + "/swat-bridge/libbridge.so");
    }

    public static native long createContext(String _source);

    public static native void setIntArg(long ctx, int index, int arg);

    public static native void setIntArrayArg(long ctx, int index, int[] arg);

    public static native void fetchIntArrayArg(long ctx, int index, int[] arg);

    public static native void run(long ctx, int range);
}
