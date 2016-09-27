/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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

    public static native int usingCuda();

    public static native long createSwatContext(String label, String _source,
            long dev_ctx, int host_thread_index, boolean requiresDouble,
            boolean requiresHeap, int max_n_buffered);
    public static native void initNativeOutBuffers(int nPrealloc,
            int[] bufferSizes, int[]  bufferArgIndices, int nBuffers, long ctx);
    public static native void cleanupKernelContext(long kernel_ctx);
    public static native void resetSwatContext(long ctx);
    public static native void cleanupSwatContext(long ctx, long dev_ctx,
            int stage, int partition);
    public static native void cleanupGlobalArguments(long ctx, long dev_ctx);
    public static native long getActualDeviceContext(int device_index,
            int heaps_per_device, int heap_size,
            double perc_high_performance_buffers, boolean createCpuContexts);
    public static native void postKernelCleanup(long ctx);
    public static native int getDeviceHintFor(int rdd, int partition,
            int offset, int component);
    public static native int getDeviceToUse(int hint, int host_thread_index,
            int heaps_per_device, int heap_size,
            double perc_high_performance_buffers, boolean createCpuContexts);
    public static native int getDevicePointerSizeInBytes(long dev_ctx);

    public static native void setIntArg(long ctx, int index, int arg);

    public static native boolean setIntArrayArgImpl(long ctx, long dev_ctx, int index,
            int[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer, boolean persistent);
    public static native boolean setDoubleArrayArgImpl(long ctx, long dev_ctx,
            int index, double[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer, boolean persistent);
    public static native boolean setFloatArrayArgImpl(long ctx, long dev_ctx,
            int index, float[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer, boolean persistent);
    public static native boolean setByteArrayArgImpl(long ctx, long dev_ctx, int index,
            byte[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer, boolean persistent);
    public static native void setNullArrayArg(long ctx, int index);
    
    public static native boolean setArrayArgImpl(long ctx, long dev_ctx,
            int index, java.lang.Object arg, int argLength, int argEleLength,
            long broadcastId, int rddid, int partitionid, int offset,
            int component, boolean persistent);

    public static native void pinnedToJVMArray(long kernel_ctx, Object jvmArray,
            long pinned, int nbytes);
    public static native void fillHeapBuffersFromKernelContext(long kernel_ctx,
            long[] jvmArr, int maxHeaps);
    public static native int getNLoaded(long kernel_ctx);

    public static native void addFreedNativeBuffer(long ctx, long dev_ctx, int buffer_id);
    public static native int waitForFreedNativeBuffer(long ctx, long dev_ctx);
    public static native void enqueueBufferFreeCallback(long ctx, long dev_ctx, int buffer_id);

    public static native long waitForFinishedKernel(long ctx, long dev_ctx, int seq_no);
    public static native long run(long ctx, long dev_ctx,
            int range, int local_size, int iterArgNum,
            int heapArgStart, int maxHeaps, int nativeOutputBufferId);
    public static native void waitOnBufferReady(long kernel_complete);
    public static native int getOutputBufferIdFromKernelCtx(long kernel_ctx);

    public static native long clMallocImpl(long dev_ctx, long nbytes);
    public static native void clFree(long clBuffer, long dev_ctx);
    public static native long pin(long dev_ctx, long region);
    public static native void setNativePinnedArrayArg(long ctx, long dev_ctx,
            int index, long pinned, long region, long nbytes);
    public static native void setOutArrayArg(long ctx, long dev_ctx, int index,
            long region);

    public static native void setIntArgByName(long ctx, int index, Object obj, String name);
    public static native void setDoubleArgByName(long ctx, int index, Object obj, String name);
    public static native void setFloatArgByName(long ctx, int index, Object obj, String name);

    public static native boolean setArgUnitialized(long ctx, long dev_ctx,
            int index, long size, boolean persistent);

    public static native void setupGlobalArguments(long ctx, long dev_ctx);
    public static native void cleanupArguments(long ctx);

    public static native boolean tryCache(long ctx, long dev_ctx, int index,
            long broadcastId, int rddid, int partitionid, int offsetid,
            int componentid, int ncomponents, boolean persistent);
    public static native void releaseAllPendingRegions(long ctx);

    public static native int getMaxOffsetOfStridedVectors(int nVectors,
            long sizesBuffer, long offsetsBuffer, int tiling);

    public static native void transferOverflowDenseVectorBuffers(long dstValues,
            long dstSizes, long dstOffsets, long srcValues, long srcSizes,
            long srcOffsets, int vectorsUsed, int elementsUsed,
            int leftoverVectors, int leftoverElements);
    public static native void transferOverflowSparseVectorBuffers(long dstValues, long dstIndices,
            long dstSizes, long dstOffsets, long srcValues, long srcIndices, long srcSizes,
            long srcOffsets, int vectorsUsed, int elementsUsed,
            int leftoverVectors, int leftoverElements);
    public static native void transferOverflowPrimitiveArrayBuffers(long dstValues,
            long dstSizes, long dstOffsets, long srcValues, long srcSizes,
            long srcOffsets, int vectorsUsed, int elementsUsed,
            int leftoverVectors, int leftoverElements, int primitiveElementSize);

    public static native int serializeStridedDenseVectorsToNativeBuffer(
            long buffer, int position, long capacity, long sizesBuffer,
            long offsetsBuffer, int buffered, int vectorCapacity,
            org.apache.spark.mllib.linalg.DenseVector[] vectors,
            int[] vectorSizes, int nToSerialize, int tiling);
    public static native int serializeStridedSparseVectorsToNativeBuffer(
            long valuesBuffer, long indicesBuffer, int position, long capacity, long sizesBuffer,
            long offsetsBuffer, int buffered, int vectorCapacity,
            org.apache.spark.mllib.linalg.SparseVector[] vectors,
            int[] vectorSizes, int nToSerialize, int tiling);
    public static native int serializeStridedPrimitiveArraysToNativeBuffer(
            long buffer, int position, long capacity, long sizesBuffer,
            long offsetsBuffer, int buffered, int vectorCapacity,
            Object[] vectors, int[] vectorLengths, int nToSerialize, int tiling,
            int primitiveElementSize);

    public static native boolean setNativeArrayArgImpl(long ctx, long dev_ctx,
        int index, long buffer, int len, long broadcast, int rdd,
        int partition, int offset, int component, boolean persistent, boolean blocking);

    public static native Object getVectorValuesFromOutputBuffers(
            long[] heapBuffers, long infoBuffer, int slot, int structSize,
            int offsetOffset, int offsetSize, int sizeOffset, int iterOffset,
            boolean isIndices);
    public static native Object getArrayValuesFromOutputBuffers(long[] heapBuffers,
            long infoBuffer, long itersBuffer, int slot, int primitiveType);

    public static native void deserializeStridedValuesFromNativeArray(
            Object[] bufferTo, int nToBuffer, long valuesBuffer,
            long sizesBuffer, long offsetsBuffer, int index, int tiling);
    public static native void deserializeStridedIndicesFromNativeArray(
            Object[] bufferTo, int nToBuffer, long indicesBuffer,
            long sizesBuffer, long offsetsBuffer, int index, int tiling);

    public static native double[] deserializeChunkedValuesFromNativeArray(
            long buffer, long infoBuffer, int offsetOffset, int sizeOffset,
            int devicePointerSize);
    public static native int[] deserializeChunkedIndicesFromNativeArray(
            long buffer, long infoBuffer, int offsetOffset, int sizeOffset,
            int devicePointerSize);

    public static native void copyNativeArrayToJVMArray(long nativeBuffer,
            int nativeOffset, Object arr, int size);
    public static native void copyJVMArrayToNativeArray(long nativeBuffer,
            int nativeOffset, Object arr, int arrOffset, int size);

    public static native void storeNLoaded(int rddid, int partitionid,
            int offsetid, int nloaded);
    public static native int fetchNLoaded(int rddid, int partitionid,
            int offsetid);

    public static native int getCurrentSeqNo(long ctx);

    public static long clMalloc(long dev_ctx, int nbytes) throws OpenCLOutOfMemoryException {
        final long buffer = clMallocImpl(dev_ctx, nbytes);
        if (buffer == 0L) {
            throw new OpenCLOutOfMemoryException(nbytes);
        }
        return buffer;
    }

    public static void setNativeArrayArg(long ctx, long dev_ctx,
        int index, long buffer, int len, long broadcast, int rdd,
        int partition, int offset, int component, boolean persistent, boolean blocking)
        throws OpenCLOutOfMemoryException {
      final boolean success = setNativeArrayArgImpl(ctx, dev_ctx, index, buffer,
          len, broadcast, rdd, partition, offset, component, persistent, blocking);
      if (!success) {
          throw new OpenCLOutOfMemoryException(len);
      }
    }

    public static void setIntArrayArg(long ctx, long dev_ctx, int index,
            int[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer,
            boolean persistent) throws OpenCLOutOfMemoryException {
        final boolean success = setIntArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component, buffer, persistent);
        if (!success) {
          throw new OpenCLOutOfMemoryException(argLength * 4);
        }
    }

    public static void setDoubleArrayArg(long ctx, long dev_ctx,
            int index, double[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer,
            boolean persistent) throws OpenCLOutOfMemoryException {
        final boolean success = setDoubleArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component, buffer, persistent);
        if (!success) {
          throw new OpenCLOutOfMemoryException(argLength * 8);
        }
    }

    public static void setFloatArrayArg(long ctx, long dev_ctx,
            int index, float[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer,
            boolean persistent) throws OpenCLOutOfMemoryException {
        final boolean success = setFloatArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component, buffer, persistent);
        if (!success) {
          throw new OpenCLOutOfMemoryException(argLength * 4);
        }
    }

    public static void setByteArrayArg(long ctx, long dev_ctx, int index,
            byte[] arg, int argLength, long broadcastId, int rddid,
            int partitionid, int offset, int component, long buffer,
            boolean persistent) throws OpenCLOutOfMemoryException {
        final boolean success = setByteArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, broadcastId, rddid, partitionid, offset, component, buffer, persistent);
        if (!success) {
            throw new OpenCLOutOfMemoryException(argLength);
        }
    }

    public static void setArrayArg(long ctx, long dev_ctx,
            int index, java.lang.Object arg, int argLength, int argEleLength,
            long broadcastId, int rddid, int partitionid, int offset,
            int component, boolean persistent) throws OpenCLOutOfMemoryException {
        final boolean success = setArrayArgImpl(ctx, dev_ctx, index, arg,
            argLength, argEleLength, broadcastId, rddid, partitionid, offset,
            component, persistent);
        if (!success) {
            throw new OpenCLOutOfMemoryException(argLength * argEleLength);
        }
    }

    /*
     * Only used for setting values captured in the closure (which can be a
     * broadcast variable or just a plain old captured array/scalar). So we
     * always want to persist.
     */
    public static int setArgByNameAndType(long ctx, long dev_ctx, int index, Object obj,
            String name, String desc, Entrypoint entryPoint,
            boolean isBroadcast) throws OpenCLOutOfMemoryException {
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
            // final boolean lengthUsed = entryPoint.getArrayFieldArrayLengthUsed().contains(name);
            final boolean lengthUsed = true;
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
                        ((int[])fieldInstance).length, broadcastId, -1, -1, -1, 0, 0, true);
                argsUsed = (lengthUsed ? 2 : 1);
            } else if (primitiveType.equals("F")) {
                setFloatArrayArg(ctx, dev_ctx, index, (float[])fieldInstance,
                        ((float[])fieldInstance).length, broadcastId, -1, -1, -1, 0, 0, true);
                argsUsed = (lengthUsed ? 2 : 1);
            } else if (primitiveType.equals("D")) {
                setDoubleArrayArg(ctx, dev_ctx, index, (double[])fieldInstance,
                        ((double[])fieldInstance).length, broadcastId, -1, -1, -1, 0, 0, true);
                argsUsed = (lengthUsed ? 2 : 1);
            } else if (primitiveType.equals("B")) {
                setByteArrayArg(ctx, dev_ctx, index, (byte[])fieldInstance,
                        ((byte[])fieldInstance).length, broadcastId, -1, -1, -1, 0, 0, true);
                argsUsed = (lengthUsed ? 2 : 1);
            } else {
              final String arrayElementTypeName = ClassModel.convert(
                  primitiveType, "", true).trim();
              final int argsUsedForData = OpenCLBridgeWrapper.setObjectTypedArrayArg(ctx,
                      dev_ctx, index, fieldInstance, arrayElementTypeName,
                      true, entryPoint, new CLCacheID(broadcastId, 0));
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

    public static void printStack() {
        for (StackTraceElement ele : Thread.currentThread().getStackTrace()) {
            System.err.println(ele.toString());
        }
        System.err.println();
    }
}
