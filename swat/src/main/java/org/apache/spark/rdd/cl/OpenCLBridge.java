package org.apache.spark.rdd.cl;

import com.amd.aparapi.internal.model.ClassModel;
import com.amd.aparapi.internal.model.Entrypoint;

import java.lang.reflect.Field;

public class OpenCLBridge {
    static {
        String swatHome = System.getenv("SWAT_HOME");
        assert(swatHome != null);
        System.load(swatHome + "/swat-bridge/libbridge.so");
    }

    public static native long createContext(String _source, boolean requiresDouble);

    public static native void setIntArg(long ctx, int index, int arg);

    public static native void setIntArrayArg(long ctx, int index, int[] arg);
    public static native void setDoubleArrayArg(long ctx, int index, double[] arg);
    public static native void setFloatArrayArg(long ctx, int index, float[] arg);
    public static native void setByteArrayArg(long ctx, int index, byte[] arg);

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

    public static void setArgByNameAndType(long ctx, int index, Object obj, String name, String desc,
            Entrypoint entryPoint) {
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
              final String arrayElementTypeName = ClassModel.convert(
                  primitiveType, "", true).trim();
              final Field field;
              try {
                field = obj.getClass().getDeclaredField(name);
                field.setAccessible(true);
              } catch (NoSuchFieldException n) {
                throw new RuntimeException(n);
              }

              final Object fieldInstance;
              try {
                fieldInstance = field.get(obj);
              } catch (IllegalAccessException i) {
                throw new RuntimeException(i);
              }
              OpenCLBridgeWrapper.setObjectTypedArrayArg(ctx, index,
                  fieldInstance, arrayElementTypeName, entryPoint);
            }
        } else {
            throw new RuntimeException("Unsupported type: " + desc);
        }
    }
}
