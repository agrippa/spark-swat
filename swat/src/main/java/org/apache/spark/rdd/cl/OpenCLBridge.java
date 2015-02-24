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
    public static native void setDoubleArrayArg(long ctx, int index, double[] arg);
    public static native void setFloatArrayArg(long ctx, int index, float[] arg);

    public static native void fetchIntArrayArg(long ctx, int index, int[] arg);
    public static native void fetchDoubleArrayArg(long ctx, int index, double[] arg);
    public static native void fetchFloatArrayArg(long ctx, int index, float[] arg);

    public static native void run(long ctx, int range);

    public static native void setIntArgByName(long ctx, int index, Object obj, String name);
    public static native void setDoubleArgByName(long ctx, int index, Object obj, String name);
    public static native void setFloatArgByName(long ctx, int index, Object obj, String name);

    public static native void setIntArrayArgByName(long ctx, int index, Object obj, String name);
    public static native void setDoubleArrayArgByName(long ctx, int index, Object obj, String name);
    public static native void setFloatArrayArgByName(long ctx, int index, Object obj, String name);

    public static void setArgByNameAndType(long ctx, int index, Object obj, String name, String desc) {
        if (desc.equals("I")) {
            setIntArgByName(ctx, index, obj, name);
        } else if (desc.equals("D")) {
            setDoubleArgByName(ctx, index, obj, name);
        } else if (desc.equals("F")) {
            setFloatArgByName(ctx, index, obj, name);
        } else if (desc.startsWith("[")) {
            // Array-typed field
            String primitiveType = desc.substring(1);
            if (primitiveType.equals("I")) {
                setIntArrayArgByName(ctx, index, obj, name);
            } else if (primitiveType.equals("F")) {
                setFloatArrayArgByName(ctx, index, obj, name);
            } else if (primitiveType.equals("D")) {
                setDoubleArrayArgByName(ctx, index, obj, name);
            } else {
              throw new RuntimeException("Unsupported array type: " + desc);
            }
        } else {
            throw new RuntimeException("Unsupported type: " + desc);
        }
    }
}
