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
    public static native long getActualDeviceContext(int device_index);
    public static native void postKernelCleanup(long ctx);
    public static native int getDeviceHintFor(int rdd, int partition,
            int offset, int component);
    public static native int getDeviceToUse(int hint, int host_thread_index);
    public static native int getDevicePointerSizeInBytes(long dev_ctx);

    public static native void setIntArg(long ctx, int index, int arg);

    public static native boolean setIntArrayArgImpl(long ctx, long dev_ctx, int index,
            int[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native boolean setDoubleArrayArgImpl(long ctx, long dev_ctx,
            int index, double[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native boolean setFloatArrayArgImpl(long ctx, long dev_ctx,
            int index, float[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native boolean setByteArrayArgImpl(long ctx, long dev_ctx, int index,
            byte[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component);
    public static native void setNullArrayArg(long ctx, int index);
    
    public static native boolean setArrayArgImpl(long ctx, long dev_ctx,
            int index, java.lang.Object arg, int argLength, int argEleLength,
            long broadcastId, int rddid, int partitionid, int offset,
            int component);

    public static native void fetchIntArrayArg(long ctx, long dev_ctx,
            int index, int[] arg, int argLength);
    public static native void fetchDoubleArrayArg(long ctx, long dev_ctx,
            int index, double[] arg, int argLength);
    public static native void fetchFloatArrayArg(long ctx, long dev_ctx,
            int index, float[] arg, int argLength);
    public static native void fetchByteArrayArg(long ctx, long dev_ctx,
            int index, byte[] arg, int argLength);

    public static native void run(long ctx, long dev_ctx, int range,
            boolean isWorkSharing);

    public static native void setIntArgByName(long ctx, int index, Object obj, String name);
    public static native void setDoubleArgByName(long ctx, int index, Object obj, String name);
    public static native void setFloatArgByName(long ctx, int index, Object obj, String name);

    public static native boolean setArgUnitialized(long ctx, long dev_ctx,
            int index, long size);

    public static native int createHeapImpl(long ctx, long dev_ctx, int index,
            int size, int max_n_buffered);
    public static native void resetHeap(long ctx, long dev_ctx,
            int starting_argnum);

    public static native boolean tryCache(long ctx, long dev_ctx, int index,
            long broadcastId, int rddid, int partitionid, int offsetid,
            int componentid, int ncomponents);
    public static native void manuallyRelease(long ctx, long dev_ctx,
            int startingIndexInclusive, int endingIndexExclusive);

    public static native long nativeMalloc(long nbytes);
    public static native void nativeFree(long buffer);
    public static native int serializeStridedDenseVectorsToNativeBuffer(
            long buffer, int position, long capacity,
            org.apache.spark.mllib.linalg.DenseVector[] vectors,
            int nToSerialize, int tiling);
    public static native boolean setNativeArrayArg(long ctx, long dev_ctx,
        int index, long buffer, int len, long broadcast, int rdd,
        int partition, int offset, int component);
    public static native void fillFromNativeArray(double[] vectorArr,
        int vectorSize, int vectorOffset, int tiling, long buffer);

    public static native void storeNLoaded(int rddid, int partitionid, int offsetid, int nloaded);
    public static native int fetchNLoaded(int rddid, int partitionid, int offsetid);

    public static int createHeap(long ctx, long dev_ctx, int index,
            int size, int max_n_buffered) throws OpenCLOutOfMemoryException {
      final int argsUsed = createHeapImpl(ctx, dev_ctx, index, size,
          max_n_buffered);
      if (argsUsed == -1) {
        throw new OpenCLOutOfMemoryException();
      }
      return argsUsed;
    }

    public static void setIntArrayArg(long ctx, long dev_ctx, int index,
            int[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component) throws OpenCLOutOfMemoryException {
        final boolean success = setIntArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component);
        if (!success) {
          throw new OpenCLOutOfMemoryException();
        }
    }

    public static void setDoubleArrayArg(long ctx, long dev_ctx,
            int index, double[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component) throws OpenCLOutOfMemoryException {
        final boolean success = setDoubleArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component);
        if (!success) {
          throw new OpenCLOutOfMemoryException();
        }
    }

    public static void setFloatArrayArg(long ctx, long dev_ctx,
            int index, float[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component) throws OpenCLOutOfMemoryException {
        final boolean success = setFloatArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component);
        if (!success) {
          throw new OpenCLOutOfMemoryException();
        }
    }

    public static void setByteArrayArg(long ctx, long dev_ctx, int index,
            byte[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component) throws OpenCLOutOfMemoryException {
        final boolean success = setByteArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component);
        if (!success) {
            throw new OpenCLOutOfMemoryException();
        }
    }

    public static void setArrayArg(long ctx, long dev_ctx,
            int index, java.lang.Object arg, int argLength, int argEleLength,
            long broadcastId, int rddid, int partitionid, int offset,
            int component) throws OpenCLOutOfMemoryException {
        final boolean success = setArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, argEleLength, broadcastId, rddid, partitionid, offset,
            component);
        if (!success) {
            throw new OpenCLOutOfMemoryException();
        }
    }

    public static int setArgByNameAndType(long ctx, long dev_ctx, int index, Object obj,
            String name, String desc, Entrypoint entryPoint, boolean isBroadcast,
            ByteBufferCache bbCache) throws OpenCLOutOfMemoryException {
        final int argsUsed;
        if (desc.equals("I")) {
            setIntArgByName(ctx, index, obj, name);
            argsUsed = 1;
        } else if (desc.equals("D")) {
            setDoubleArgByName(ctx, index, obj, name);
            argsUsed = 1;
        } else if (desc.equals("F")) {
            setFloatArgByName(ctx, index, obj, name);
            argsUsed = 1;
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

            int broadcastId = -1;
            if (isBroadcast) {
                broadcastId = (int)OpenCLBridgeWrapper.getBroadcastId(fieldInstance);
                fieldInstance = OpenCLBridgeWrapper.unwrapBroadcastedArray(
                    fieldInstance);
            }

            String primitiveType = desc.substring(1);
            if (primitiveType.equals("I")) {
                setIntArrayArg(ctx, dev_ctx, index, (int[])fieldInstance,
                        ((int[])fieldInstance).length, broadcastId, -1, -1, -1, 0);
                argsUsed = (lengthUsed ? 2 : 1);
            } else if (primitiveType.equals("F")) {
                setFloatArrayArg(ctx, dev_ctx, index, (float[])fieldInstance,
                        ((float[])fieldInstance).length, broadcastId, -1, -1, -1, 0);
                argsUsed = (lengthUsed ? 2 : 1);
            } else if (primitiveType.equals("D")) {
                setDoubleArrayArg(ctx, dev_ctx, index, (double[])fieldInstance,
                        ((double[])fieldInstance).length, broadcastId, -1, -1, -1, 0);
                argsUsed = (lengthUsed ? 2 : 1);
            } else {
              final String arrayElementTypeName = ClassModel.convert(
                  primitiveType, "", true).trim();
              final int argsUsedForData = OpenCLBridgeWrapper.setObjectTypedArrayArg(ctx,
                      dev_ctx, index, fieldInstance, arrayElementTypeName,
                      true, entryPoint, new CLCacheID(broadcastId, 0), bbCache);
              argsUsed = (lengthUsed ? argsUsedForData + 1 : argsUsedForData);
            }

            if (lengthUsed) {
                setIntArg(ctx, index + argsUsed - 1,
                    OpenCLBridgeWrapper.getArrayLength(fieldInstance));
            }
        } else {
            throw new RuntimeException("Unsupported type: " + desc);
        }

        return argsUsed;
    }

    public static Constructor getDefaultConstructor(Class clazz) {
        Constructor[] allConstructors = clazz.getDeclaredConstructors();
        for (Constructor c : allConstructors) {
            if (c.getParameterTypes().length == 0) {
                c.setAccessible(true);
                return c;
            }
        }
        throw new RuntimeException("Unable to find default constructor for " +
                clazz.getName());
    }
}
