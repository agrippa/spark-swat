package org.apache.spark.rdd.cl;

import java.util.Map;
import java.util.HashMap;
import java.net.*;

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

    public static native long createSwatContext(String label, String _source,
            long dev_ctx, int host_thread_index, boolean requiresDouble,
            boolean requiresHeap);
    public static native void cleanupSwatContext(long ctx);
    public static native long getDeviceContext(int host_thread_index);
    public static native void postKernelCleanup(long ctx);

    public static native void setIntArg(long ctx, int index, int arg);

    public static native void setIntArrayArg(long ctx, long dev_ctx, int index,
            int[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native void setDoubleArrayArg(long ctx, long dev_ctx,
            int index, double[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native void setFloatArrayArg(long ctx, long dev_ctx,
            int index, float[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native void setByteArrayArg(long ctx, long dev_ctx, int index,
            byte[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native void setNullArrayArg(long ctx, int index);

    public static native void fetchIntArrayArg(long ctx, long dev_ctx,
            int index, int[] arg, int argLength);
    public static native void fetchDoubleArrayArg(long ctx, long dev_ctx,
            int index, double[] arg, int argLength);
    public static native void fetchFloatArrayArg(long ctx, long dev_ctx,
            int index, float[] arg, int argLength);
    public static native void fetchByteArrayArg(long ctx, long dev_ctx,
            int index, byte[] arg, int argLength);

    public static native void run(long ctx, long dev_ctx, int range);

    public static native void setIntArgByName(long ctx, int index, Object obj, String name);
    public static native void setDoubleArgByName(long ctx, int index, Object obj, String name);
    public static native void setFloatArgByName(long ctx, int index, Object obj, String name);

    public static native void setIntArrayArgByName(long ctx, long dev_ctx,
            int index, Object obj, String name);
    public static native void setDoubleArrayArgByName(long ctx, long dev_ctx,
            int index, Object obj, String name);
    public static native void setFloatArrayArgByName(long ctx, long dev_ctx,
            int index, Object obj, String name);

    public static native void setArgUnitialized(long ctx, long dev_ctx,
            int index, long size);

    public static native int createHeap(long ctx, long dev_ctx, int index,
            int size, int max_n_buffered);
    public static native void resetHeap(long ctx, long dev_ctx,
            int starting_argnum);

    public static int setArgByNameAndType(long ctx, long dev_ctx, int index, Object obj,
            String name, String desc, Entrypoint entryPoint, boolean isBroadcast,
            ByteBufferCache bbCache) {
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

            long broadcastId = -1;
            if (isBroadcast) {
                // try {
                // System.err.println("SWAT Looking at broadcast " +
                //         OpenCLBridgeWrapper.getBroadcastId(fieldInstance) + " on " +
                //         InetAddress.getLocalHost().getHostName() + " in " +
                //         Thread.currentThread().getName());
                // } catch (UnknownHostException u) {
                //     throw new RuntimeException(u);
                // }
                broadcastId = OpenCLBridgeWrapper.getBroadcastId(fieldInstance);
                fieldInstance = OpenCLBridgeWrapper.unwrapBroadcastedArray(
                    fieldInstance);
            }

            String primitiveType = desc.substring(1);
            if (primitiveType.equals("I")) {
                setIntArrayArg(ctx, dev_ctx, index, (int[])fieldInstance,
                        ((int[])fieldInstance).length, broadcastId, -1, -1, -1, -1);
            } else if (primitiveType.equals("F")) {
                setFloatArrayArg(ctx, dev_ctx, index, (float[])fieldInstance,
                        ((float[])fieldInstance).length, broadcastId, -1, -1, -1, -1);
            } else if (primitiveType.equals("D")) {
                setDoubleArrayArg(ctx, dev_ctx, index, (double[])fieldInstance,
                        ((double[])fieldInstance).length, broadcastId, -1, -1, -1, -1);
            } else {
              final String arrayElementTypeName = ClassModel.convert(
                  primitiveType, "", true).trim();
              argsUsed = OpenCLBridgeWrapper.setObjectTypedArrayArg(ctx,
                      dev_ctx, index, fieldInstance, arrayElementTypeName, true,
                      entryPoint, broadcastId, -1, -1, -1, bbCache);
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

    private static final Map<Class, Constructor> constructorCache =
        new HashMap<Class, Constructor>();

    public static <T> T constructObjectFromDefaultConstructor(Class<T> clazz)
            throws InstantiationException, IllegalAccessException,
                   InvocationTargetException {
        Constructor defaultConstructor = null;
        if (constructorCache.containsKey(clazz)) {
            defaultConstructor = constructorCache.get(clazz);
        } else {
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
            constructorCache.put(clazz, defaultConstructor);
        }

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
