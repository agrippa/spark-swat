package org.apache.spark.rdd.cl;

import com.amd.aparapi.internal.model.ClassModel;
import com.amd.aparapi.internal.model.Entrypoint;

import java.lang.reflect.Field;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class OpenCLBridge {
    static {
        String swatHome = System.getenv("SWAT_HOME");
        assert(swatHome != null);
        System.load(swatHome + "/swat-bridge/libbridge.so");
    }

    public static native long createContext(String _source,
        boolean requiresDouble, boolean requiresHeap);

    public static native void setIntArg(long ctx, int index, int arg);

    public static native void setIntArrayArg(long ctx, int index, int[] arg);
    public static native void setDoubleArrayArg(long ctx, int index, double[] arg);
    public static native void setFloatArrayArg(long ctx, int index, float[] arg);
    public static native void setByteArrayArg(long ctx, int index, byte[] arg);
    public static native void setNullArrayArg(long ctx, int index);

    public static native void fetchIntArrayArg(long ctx, int index, int[] arg);
    public static native void fetchDoubleArrayArg(long ctx, int index, double[] arg);
    public static native void fetchFloatArrayArg(long ctx, int index, float[] arg);
    public static native void fetchByteArrayArg(long ctx, int index, byte[] arg);

    public static native void run(long ctx, int range);

    public static native void setIntArgByName(long ctx, int index, Object obj, String name);
    public static native void setDoubleArgByName(long ctx, int index, Object obj, String name);
    public static native void setFloatArgByName(long ctx, int index, Object obj, String name);

    public static native void setIntArrayArgByName(long ctx, int index, Object obj, String name);
    public static native void setDoubleArrayArgByName(long ctx, int index, Object obj, String name);
    public static native void setFloatArrayArgByName(long ctx, int index, Object obj, String name);

    public static native void setArgUnitialized(long ctx, int index, long size);

    public static native int createHeap(long ctx, int index, int size, int max_n_buffered);
    public static native void resetHeap(long ctx, int starting_argnum);

    public static int setArgByNameAndType(long ctx, int index, Object obj,
            String name, String desc, Entrypoint entryPoint, boolean isBroadcast) {
        int argsUsed = 1;
        if (desc.equals("I")) {
            setIntArgByName(ctx, index, obj, name);
        } else if (desc.equals("D")) {
            setDoubleArgByName(ctx, index, obj, name);
        } else if (desc.equals("F")) {
            setFloatArgByName(ctx, index, obj, name);
        } else if (desc.startsWith("[")) {
            // Array-typed field
            final boolean lengthUsed = entryPoint.getArrayFieldArrayLengthUsed().contains(name);
            final Field field;
            try {
              field = obj.getClass().getDeclaredField(name);
              field.setAccessible(true);
            } catch (NoSuchFieldException n) {
              throw new RuntimeException(n);
            }

            Object fieldInstance;
            try {
              fieldInstance = field.get(obj);
            } catch (IllegalAccessException i) {
              throw new RuntimeException(i);
            }

            if (isBroadcast) {
                fieldInstance = OpenCLBridgeWrapper.unwrapBroadcastedArray(
                        fieldInstance);
            }

            String primitiveType = desc.substring(1);
            if (primitiveType.equals("I")) {
                setIntArrayArg(ctx, index, (int[])fieldInstance);
            } else if (primitiveType.equals("F")) {
                setFloatArrayArg(ctx, index, (float[])fieldInstance);
            } else if (primitiveType.equals("D")) {
                setDoubleArrayArg(ctx, index, (double[])fieldInstance);
            } else {
              final String arrayElementTypeName = ClassModel.convert(
                  primitiveType, "", true).trim();
              argsUsed = OpenCLBridgeWrapper.setObjectTypedArrayArg(ctx, index,
                  fieldInstance, arrayElementTypeName, true, entryPoint);
            }

            if (lengthUsed) {
                setIntArg(ctx, index + argsUsed,
                    OpenCLBridgeWrapper.getArrayLength(fieldInstance));
                argsUsed += 1;
            }
        } else {
            throw new RuntimeException("Unsupported type: " + desc);
        }

        return argsUsed;
    }

    public static <T> T constructObjectFromDefaultConstructor(Class<T> clazz)
            throws InstantiationException, IllegalAccessException,
                   InvocationTargetException {
        Constructor defaultConstructor = null;
        Constructor[] allConstructors = clazz.getDeclaredConstructors();
        for (Constructor c : allConstructors) {
            if (c.getParameterTypes().length == 0) {
                defaultConstructor = c;
                break;
            }
        }

        if (defaultConstructor == null) {
            throw new RuntimeException("Expected default constructor for " +
                    "class " + clazz.getName());
        }

        defaultConstructor.setAccessible(true);
        T newObj;
        try {
            newObj = (T)defaultConstructor.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return newObj;
    }
}
