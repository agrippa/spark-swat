#include <jni.h>
#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "bridge.h"
#include "common.h"
#include "kernel_arg.h"
#include "ocl_util.h"

static device_context *device_ctxs = NULL;
static int n_device_ctxs = 0;
static pthread_mutex_t device_ctxs_lock = PTHREAD_MUTEX_INITIALIZER;
static int *virtual_devices = NULL;
static int n_virtual_devices = 0;

#define ARG_MACRO(ltype, utype) \
JNI_JAVA(void, OpenCLBridge, set##utype##Arg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, j##ltype arg) { \
    ENTER_TRACE("set"#utype"Arg"); \
    swat_context *context = (swat_context *)lctx; \
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(arg), &arg); \
    EXIT_TRACE("set"#utype"Arg"); \
}

#define SET_ARRAY_ARG_MACRO(ltype, utype, ctype) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArrayArg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, \
         j##ltype##Array arg, jint argLength, jlong broadcastId, jint rddid, \
         jint partitionid, jint offsetid, jint componentid) { \
    ENTER_TRACE("set"#utype"ArrayArg"); \
    jsize len = argLength * sizeof(ctype); \
    device_context *dev_ctx = (device_context *)l_dev_ctx; \
    swat_context *context = (swat_context *)lctx; \
    int err = pthread_mutex_lock(&dev_ctx->lock); \
    jboolean isCopy; \
    assert(err == 0); \
    if (broadcastId >= 0 && \
            dev_ctx->broadcast_cache->find(broadcastId) != \
            dev_ctx->broadcast_cache->end()) { \
        cl_region *region = dev_ctx->broadcast_cache->at(broadcastId); \
        if (re_allocate_cl_region(region)) { \
            CHECK(clSetKernelArg(context->kernel, index, \
                        sizeof(region->sub_mem), &region->sub_mem)); \
            (*context->arguments)[index] = pair<cl_region *, bool>(region, true); \
        } else { \
            void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
            cl_region *new_region = set_kernel_arg(arr, len, index, context, \
                    dev_ctx, broadcastId, rddid); \
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
            (*dev_ctx->broadcast_cache)[broadcastId] = new_region; \
        } \
    } else if (rddid >= 0 && dev_ctx->rdd_cache->find(rdd_partition_offset( \
                    rddid, partitionid, offsetid, componentid)) != \
                dev_ctx->rdd_cache->end()) { \
        rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid); \
        cl_region *region = dev_ctx->rdd_cache->at(uuid); \
        if (re_allocate_cl_region(region)) { \
            CHECK(clSetKernelArg(context->kernel, index, \
                        sizeof(region->sub_mem), &region->sub_mem)); \
            (*context->arguments)[index] = pair<cl_region *, bool>(region, true); \
        } else { \
            void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
            cl_region *new_region = set_kernel_arg(arr, len, index, context, \
                    dev_ctx, broadcastId, rddid); \
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
            dev_ctx->rdd_cache->find(uuid)->second = new_region; \
        } \
    } else { \
        void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
        cl_region *new_region = set_kernel_arg(arr, len, index, context, \
                dev_ctx, broadcastId, rddid); \
        jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
        if (broadcastId >= 0) { \
            bool success = dev_ctx->broadcast_cache->insert( \
                    pair<jlong, cl_region *>(broadcastId, new_region)).second; \
            assert(success); \
        } else if (rddid >= 0) { \
            bool success = dev_ctx->rdd_cache->insert( \
                    pair<rdd_partition_offset, cl_region *>( \
                        rdd_partition_offset(rddid, partitionid, offsetid, \
                            componentid), new_region)).second; \
            assert(success); \
        } \
    } \
    err = pthread_mutex_unlock(&dev_ctx->lock); \
    assert(err == 0); \
    EXIT_TRACE("set"#utype"ArrayArg"); \
}

#define FETCH_ARRAY_ARG_MACRO(ltype, utype, ctype) \
JNI_JAVA(void, OpenCLBridge, fetch##utype##ArrayArg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, \
         j##ltype##Array arg, jint argLength, jlong broadcastId) { \
    ENTER_TRACE("fetch"#utype"ArrayArg"); \
    jsize len = argLength * sizeof(ctype); \
    void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL); \
    fetch_kernel_arg(arr, len, index, (swat_context *)lctx, (device_context *)l_dev_ctx); \
    jenv->ReleasePrimitiveArrayCritical(arg, arr, 0); \
    EXIT_TRACE("fetch"#utype"ArrayArg"); \
}

#define SET_PRIMITIVE_ARG_BY_NAME_MACRO(ltype, utype, desc) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArgByName) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jobject obj, \
         jstring name) { \
    ENTER_TRACE("set"#utype"ArgByName"); \
    swat_context *context = (swat_context *)lctx; \
    jclass enclosing_class = jenv->GetObjectClass(obj); \
    const char *raw_name = jenv->GetStringUTFChars(name, NULL); \
    jfieldID field = jenv->GetFieldID(enclosing_class, raw_name, desc); \
    jenv->ReleaseStringUTFChars(name, raw_name); \
    ltype val = jenv->Get##utype##Field(obj, field); \
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(val), &val); \
    EXIT_TRACE("set"#utype"ArgByName"); \
}

#define SET_ARRAY_ARG_BY_NAME_MACRO(ltype, utype, desc) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArrayArgByName) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, jobject obj, \
         jstring name) { \
    ENTER_TRACE("set"#utype"ArrayArgByName"); \
    jclass enclosing_class = jenv->GetObjectClass(obj); \
    const char *raw_name = jenv->GetStringUTFChars(name, NULL); \
    jfieldID field = jenv->GetFieldID(enclosing_class, raw_name, desc); \
    jenv->ReleaseStringUTFChars(name, raw_name); \
    jarray arr = (jarray)jenv->GetObjectField(obj, field); \
    jsize arr_length = jenv->GetArrayLength(arr) * sizeof(ltype); \
    void *arr_eles = jenv->GetPrimitiveArrayCritical((j##ltype##Array)arr, NULL); \
    set_kernel_arg(arr_eles, arr_length, index, (swat_context *)lctx, \
            (device_context *)l_dev_ctx, -1, -1); \
    jenv->ReleasePrimitiveArrayCritical((j##ltype##Array)arr, arr_eles, JNI_ABORT); \
    EXIT_TRACE("set"#utype"ArrayArgByName"); \
}


#ifdef __cplusplus
extern "C" {
#endif

#ifdef TRACE
void enter_trace(const char *lbl) {
    fprintf(stderr, "entering %s\n", lbl);
}

void exit_trace(const char *lbl) {
    fprintf(stderr, "leaving %s\n", lbl);
}
#endif

static void clSetKernelArgWrapper(swat_context *context, cl_kernel kernel,
        cl_uint arg_index, size_t arg_size, void *arg_value) {
    CHECK(clSetKernelArg(kernel, arg_index, arg_size, arg_value));
#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[arg_index] = new kernel_arg(arg_value, arg_size,
            false, false, 0);
#endif

#ifdef VERBOSE
    fprintf(stderr, "setting arg %u to value %d with size %lu\n", arg_index,
            *((int *)arg_value), arg_size);
#endif
}

static int checkExtension(char *exts, size_t ext_len, const char *ext) {
    unsigned start = 0;
    while (start < ext_len) {
        unsigned end = start;
        while (end < ext_len && exts[end] != ' ') {
            end++;
        }

        if (strncmp(exts + start, ext, end - start) == 0) {
            return 1;
        }

        start = end + 1;
    }
    return 0;
}

static int checkAllAssertions(cl_device_id device, int requiresDouble,
        int requiresHeap) {

    int requires_extension_check = (requiresDouble || requiresHeap);
    if (requires_extension_check) {
        size_t ext_len;
        CHECK(clGetDeviceInfo(device, CL_DEVICE_EXTENSIONS, 0, NULL, &ext_len));
        char *exts = (char *)malloc(ext_len);
        CHECK(clGetDeviceInfo(device, CL_DEVICE_EXTENSIONS, ext_len, exts, NULL));

        if (requiresDouble && !checkExtension(exts, ext_len, "cl_khr_fp64")) {
            return 0;
        }

        if (requiresHeap &&
                (!checkExtension(exts, ext_len, "cl_khr_global_int32_base_atomics") ||
                 !checkExtension(exts, ext_len, "cl_khr_global_int32_extended_atomics") ||
                 !checkExtension(exts, ext_len, "cl_khr_local_int32_base_atomics") ||
                 !checkExtension(exts, ext_len, "cl_khr_local_int32_extended_atomics"))) {
            return 0;
        }
    }
    return 1;
}

/*
 * Expected format:
 *
 *     export SWAT_DEVICE_CONFIG="0,0,1,2"
 *
 * The scheduler should assign devices to threads round-robin across
 * SWAT_DEVICE_CONFIG. For the above example, you would have the following
 * mapping from thread to device for 8 host threads:
 *
 *   0 -> 0
 *   1 -> 0
 *   2 -> 1
 *   3 -> 2
 *   4 -> 0
 *   5 -> 0
 *   6 -> 1
 *   7 -> 2
 *
 */
static void parseDeviceConfig(int *gpu_weight_out, int *cpu_weight_out) {
    char *gpu_weight_str = getenv("SWAT_GPU_WEIGHT");
    if (gpu_weight_str == NULL) {
        fprintf(stderr, "Error: SWAT_GPU_WEIGHT must be set in spark-env.sh\n");
        exit(1);
    }
    char *cpu_weight_str = getenv("SWAT_CPU_WEIGHT");
    if (cpu_weight_str == NULL) {
        fprintf(stderr, "Error: SWAT_CPU_WEIGHT must be set in spark-env.sh\n");
        exit(1);
    }

    int gpu_weight = atoi(gpu_weight_str);
    int cpu_weight = atoi(cpu_weight_str);

    *gpu_weight_out = gpu_weight;
    *cpu_weight_out = cpu_weight;
}

JNI_JAVA(jlong, OpenCLBridge, getDeviceContext)
        (JNIEnv *jenv, jclass clazz, int host_thread_index) {

    int cpu_weight, gpu_weight;
    parseDeviceConfig(&gpu_weight, &cpu_weight);

    /*
     * clGetPlatformIDs called inside get_num_opencl_platforms is supposed to be
     * thread-safe... That doens't stop the Intel OpenCL library from
     * segfaulting or throwing an error if it is called from multiple threads at
     * once. If I had to guess, some Intel initialization is not thread-safe
     * even though the core code of clGetPlatformIDs is. Work around is just to
     * ensure no thread makes this call at the same time using a mutex, seems to
     * work fine but this shouldn't be happening to begin with.
     */
    int perr = pthread_mutex_lock(&device_ctxs_lock);
    assert(perr == 0);

    if (device_ctxs == NULL) {
        cl_uint total_num_devices = get_total_num_devices();
        device_ctxs = (device_context *)malloc(total_num_devices * sizeof(device_context));
        n_device_ctxs = total_num_devices;

        cl_uint num_platforms = get_num_opencl_platforms();

        cl_platform_id *platforms =
            (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
        CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));

        unsigned global_device_id = 0;
        for (unsigned platform_index = 0; platform_index < num_platforms; platform_index++) {
            cl_uint num_devices = get_num_devices(platforms[platform_index]);
            cl_device_id *devices = (cl_device_id *)malloc(num_devices * sizeof(cl_device_id));
            CHECK(clGetDeviceIDs(platforms[platform_index], CL_DEVICE_TYPE_ALL,
                        num_devices, devices, NULL));

            for (unsigned i = 0; i < num_devices; i++) {
                cl_device_id curr_dev = devices[i];
#ifdef VERBOSE
                char *device_name = get_device_name(curr_dev);
                fprintf(stderr, "SWAT %d: platform %d, device %d, %s (%s)\n",
                        global_device_id, platform_index, i, device_name,
                        get_device_type_str(curr_dev));
                free(device_name);
#endif

                cl_int err;
                cl_context_properties ctx_props[] = { CL_CONTEXT_PLATFORM,
                    (cl_context_properties)platforms[platform_index], 0 };
                cl_context ctx = clCreateContext(ctx_props, 1, &curr_dev, NULL, NULL, &err);
                CHECK(err);

                cl_command_queue cmd = clCreateCommandQueue(ctx, curr_dev,
                        CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE, &err);
                CHECK(err);

                device_ctxs[global_device_id].platform = platforms[platform_index];
                device_ctxs[global_device_id].dev = curr_dev;
                device_ctxs[global_device_id].ctx = ctx;
                device_ctxs[global_device_id].cmd = cmd;

                int perr = pthread_mutex_init(&(device_ctxs[global_device_id].lock), NULL);
                assert(perr == 0);

                device_ctxs[global_device_id].allocator = init_allocator(curr_dev, ctx, cmd);

                device_ctxs[global_device_id].broadcast_cache =
                    new map<jlong, cl_region *>();
                device_ctxs[global_device_id].program_cache =
                    new map<string, cl_program>();
                device_ctxs[global_device_id].rdd_cache =
                    new map<rdd_partition_offset, cl_region *>();

                global_device_id++;

                switch (get_device_type(curr_dev)) {
                    case (CL_DEVICE_TYPE_CPU):
                        n_virtual_devices += cpu_weight;
                        break;
                    case (CL_DEVICE_TYPE_GPU):
                        n_virtual_devices += gpu_weight;
                        break;
                    default:
                        fprintf(stderr, "Unsupported device type %d\n",
                                (int)get_device_type(curr_dev));
                        exit(1);
                }
            }
            free(devices);
        }

        virtual_devices = (int *)malloc(sizeof(int) * n_virtual_devices);
        int curr_virtual_device = 0;
        for (unsigned i = 0; i < total_num_devices; i++) {
            cl_device_id curr_dev = device_ctxs[i].dev;

            int weight;
            switch (get_device_type(curr_dev)) {
                case (CL_DEVICE_TYPE_CPU):
                    weight = cpu_weight;
                    break;
                case (CL_DEVICE_TYPE_GPU):
                    weight = gpu_weight;
                    break;
                default:
                    fprintf(stderr, "Unsupported device type %d\n",
                            (int)get_device_type(curr_dev));
                    exit(1);
            }

            for (int j = 0; j < weight; j++) {
                virtual_devices[curr_virtual_device++] = i;
            }
        }
        assert(curr_virtual_device == n_virtual_devices);
    }

    device_context *my_ctx = device_ctxs +
        virtual_devices[host_thread_index % n_virtual_devices];
#ifdef VERBOSE
    fprintf(stderr, "Host thread %d using virtual device %d, physical "
            "device %d: %s\n", host_thread_index,
            host_thread_index % n_virtual_devices,
            virtual_devices[host_thread_index % n_virtual_devices], get_device_name(my_ctx->dev));
#endif

    perr = pthread_mutex_unlock(&device_ctxs_lock);
    assert(perr == 0);

    return (jlong)my_ctx;
}

JNI_JAVA(void, OpenCLBridge, cleanupSwatContext)
        (JNIEnv *jenv, jclass clazz, jlong l_ctx) {
    if (l_ctx != -1L) {
        swat_context *ctx = (swat_context *)l_ctx;
        CHECK(clReleaseKernel(ctx->kernel));
        delete ctx->arguments;
        free(ctx);
    }
}

JNI_JAVA(jlong, OpenCLBridge, createSwatContext)
        (JNIEnv *jenv, jclass clazz, jstring label, jstring source,
         jlong l_dev_ctx, jint host_thread_index, jboolean requiresDouble,
         jboolean requiresHeap) {
    ENTER_TRACE("createContext");

    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_device_id device = dev_ctx->dev;

    assert(checkAllAssertions(device, requiresDouble, requiresHeap) == 1);

#ifdef VERBOSE
    char *device_name = get_device_name(device);

    fprintf(stderr, "SWAT: Using device %s, require double? %d, "
            "require heap? %d\n", device_name, requiresDouble,
            requiresHeap);
    free(device_name);
#endif

    const char *raw_label = jenv->GetStringUTFChars(label, NULL);
    std::string label_str(raw_label);
    jenv->ReleaseStringUTFChars(label, raw_label);

    int perr = pthread_mutex_lock(&dev_ctx->lock);
    assert(perr == 0);
   
    cl_int err;
    cl_program program;
    if (dev_ctx->program_cache->find(label_str) != dev_ctx->program_cache->end()) {
        program = dev_ctx->program_cache->at(label_str);
    } else {
        const char *raw_source = jenv->GetStringUTFChars(source, NULL);
        size_t source_len = strlen(raw_source);
#ifdef BRIDGE_DEBUG
        char *store_source = (char *)malloc(source_len + 1);
        memcpy(store_source, raw_source, source_len);
        store_source[source_len] = '\0';
#endif

        size_t source_size[] = { source_len };
        program = clCreateProgramWithSource(dev_ctx->ctx, 1, &raw_source,
                source_size, &err);
        CHECK(err);

        err = clBuildProgram(program, 1, &device, NULL, NULL, NULL);
        if (err == CL_BUILD_PROGRAM_FAILURE) {
            size_t build_log_size;
            CHECK(clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG, 0,
                        NULL, &build_log_size));
            char *build_log = (char *)malloc(build_log_size + 1);
            CHECK(clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG,
                        build_log_size, build_log, NULL));
            build_log[build_log_size] = '\0';
            fprintf(stderr, "%s\n\n", raw_source);
            fprintf(stderr, "Build failure:\n%s\n", build_log);
            free(build_log);
        }
        jenv->ReleaseStringUTFChars(source, raw_source);
        CHECK(err);

        bool success = dev_ctx->program_cache->insert(pair<string, cl_program>(label_str,
                    program)).second;
        assert(success);
    }

    perr = pthread_mutex_unlock(&dev_ctx->lock);
    assert(perr == 0);

    cl_kernel kernel = clCreateKernel(program, "run", &err);
    CHECK(err);

    swat_context *context = (swat_context *)malloc(sizeof(swat_context));
    // context->program = program;
    context->kernel = kernel;
    context->host_thread_index = host_thread_index;
    context->arguments = new map<int, pair<cl_region *, bool> >();
    // context->all_allocated = new set<cl_mem>();
#ifdef BRIDGE_DEBUG
    context->debug_arguments = new map<int, kernel_arg *>();
    context->kernel_src = store_source;
    context->kernel_src_len = source_len;
#endif

    EXIT_TRACE("createContext");
    return (jlong)context;
}

static cl_region *get_mem_cached(swat_context *context, device_context *dev_ctx,
        int index, size_t size, jlong broadcastId, jint rdd) {
    cl_region *region = allocate_cl_region(size, dev_ctx->allocator);

#ifdef VERBOSE
    fprintf(stderr, "%d: Allocating %p, %lu bytes for index=%d\n",
            context->host_thread_index, region, size, index);
#endif

    (*context->arguments)[index] = pair<cl_region *, bool>(region, broadcastId >= 0 || rdd >= 0);
    return region;
}

static cl_region *set_kernel_arg(void *host, size_t len, int index,
        swat_context *context, device_context *dev_ctx, jlong broadcastId, jint rdd) {
    cl_region *region = get_mem_cached(context, dev_ctx, index, len, broadcastId, rdd);

    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem, CL_TRUE, 0, len, host,
                0, NULL, NULL));

    CHECK(clSetKernelArg(context->kernel, index, sizeof(region->sub_mem), &region->sub_mem));

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[index] = new kernel_arg(host, len, true, false,
            0);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a memory buffer of size %lu\n", index,
            len);
#endif
    return region;
}

static void fetch_kernel_arg(void *host, size_t len, int index,
        swat_context *context, device_context *dev_ctx) {
    assert(context->arguments->find(index) != context->arguments->end());
    cl_mem mem = context->arguments->at(index).first->sub_mem;

    CHECK(clEnqueueReadBuffer(dev_ctx->cmd, mem, CL_TRUE, 0, len, host,
                0, NULL, NULL));
}

ARG_MACRO(int, Int)

SET_PRIMITIVE_ARG_BY_NAME_MACRO(int, Int, "I")
SET_PRIMITIVE_ARG_BY_NAME_MACRO(double, Double, "D")
SET_PRIMITIVE_ARG_BY_NAME_MACRO(float, Float, "F")

SET_ARRAY_ARG_BY_NAME_MACRO(int, Int, "[I")
SET_ARRAY_ARG_BY_NAME_MACRO(double, Double, "[D")
SET_ARRAY_ARG_BY_NAME_MACRO(float, Float, "[F")

SET_ARRAY_ARG_MACRO(int, Int, int)
SET_ARRAY_ARG_MACRO(double, Double, double)
SET_ARRAY_ARG_MACRO(float, Float, float)
SET_ARRAY_ARG_MACRO(byte, Byte, jbyte)

FETCH_ARRAY_ARG_MACRO(int, Int, int)
FETCH_ARRAY_ARG_MACRO(double, Double, double)
FETCH_ARRAY_ARG_MACRO(float, Float, float)
FETCH_ARRAY_ARG_MACRO(byte, Byte, jbyte)

JNI_JAVA(void, OpenCLBridge, postKernelCleanup)
        (JNIEnv *jenv, jclass clazz, jlong lctx) {
    swat_context *ctx = (swat_context *)lctx;
    cl_allocator *allocator = NULL;

    for (map<int, pair<cl_region *, bool> >::iterator i = ctx->arguments->begin(),
            e = ctx->arguments->end(); i != e; i++) {
        cl_region *region = i->second.first;
        if (region) {
            if (allocator) {
                assert(allocator == region->grandparent->allocator);
            } else {
                allocator = region->grandparent->allocator;
            }
            free_cl_region(region, i->second.second);
        }
    }

    if (allocator) {
        bump_time(allocator);
    }
}

JNI_JAVA(void, OpenCLBridge, setArgUnitialized)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum,
         jlong size) {
    ENTER_TRACE("setArgUnitialized");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;

    cl_region *region = get_mem_cached(context, dev_ctx, argnum, size, -1, -1);
    CHECK(clSetKernelArg(context->kernel, argnum, sizeof(region->sub_mem), &region->sub_mem));

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[argnum] = new kernel_arg(NULL, size, true,
            false, 0);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to an unitialized value with size %ld\n",
            argnum, size);
#endif

    EXIT_TRACE("setArgUnitialized");
}

JNI_JAVA(void, OpenCLBridge, setNullArrayArg)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint argnum) {
    ENTER_TRACE("setNullArrayArg");

    cl_mem none = 0x0;
    swat_context *context = (swat_context *)lctx;
    CHECK(clSetKernelArg(context->kernel, argnum, sizeof(none), &none));

    (*context->arguments)[argnum] = pair<cl_region *, bool>(NULL, false);

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[argnum] = new kernel_arg(&none, sizeof(none),
            false, false, 0);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a null value\n", argnum);
#endif

    EXIT_TRACE("setNullArrayArg");
}


JNI_JAVA(int, OpenCLBridge, createHeap)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum, jint size,
         jint max_n_buffered) {
    ENTER_TRACE("createHeap");

    unsigned zero = 0;
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    jint local_argnum = argnum;

    // The heap
    cl_region *region = get_mem_cached(context, dev_ctx, local_argnum, size, -1, -1);
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL, size,
            true, false, 0);
#endif

    // free_index
    region = get_mem_cached(context, dev_ctx, local_argnum, sizeof(zero), -1, -1);
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem, CL_TRUE, 0, sizeof(zero),
                &zero, 0, NULL, NULL));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(zero), true, true, zero);
#endif

    // heap_size
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(size), &size));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(&size,
            sizeof(size), false, false, 0);
#endif

    // processing_succeeded
    region = get_mem_cached(context, dev_ctx, local_argnum, sizeof(int) * max_n_buffered, -1, -1);
#ifdef VERBOSE
    fprintf(stderr, "%d: Creating %p, %lu bytes for processing_succeeded\n",
            context->host_thread_index, region->sub_mem,
            sizeof(int) * max_n_buffered);
#endif
    cl_event fill_event;
    int *zeros = (int *)malloc(max_n_buffered * sizeof(int));
    memset(zeros, 0x00, max_n_buffered * sizeof(int));
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem, CL_FALSE, 0,
                max_n_buffered * sizeof(int), zeros, 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    CHECK(clWaitForEvents(1, &fill_event));
    free(zeros);
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(int) * max_n_buffered, true, true, zero);
#endif

    // any_failed
    region = get_mem_cached(context, dev_ctx, local_argnum, sizeof(unsigned), -1, -1);
#ifdef VERBOSE
    fprintf(stderr, "%d: Creating %p, %lu bytes for any_failed\n",
            context->host_thread_index, region->sub_mem, sizeof(unsigned));
#endif
    unsigned one_zero = 0;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem, CL_FALSE, 0, sizeof(unsigned),
                &one_zero, 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    CHECK(clWaitForEvents(1, &fill_event));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(unsigned), true, true, zero);
#endif
#ifdef VERBOSE
    fprintf(stderr, "Creating heap at %d->%d (inclusive)\n", argnum,
            local_argnum - 1);
#endif

    EXIT_TRACE("createHeap");
    return (local_argnum - argnum); // Number of kernel arguments added
}

JNI_JAVA(void, OpenCLBridge, resetHeap)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint starting_argnum) {
    ENTER_TRACE("resetHeap");

    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    map<int, pair<cl_region *, bool> > *arguments = context->arguments;
    int free_index_arg_index = starting_argnum + 1;
    int any_failed_arg_index = starting_argnum + 4;

    assert(arguments->find(free_index_arg_index) != arguments->end());
    assert(arguments->find(any_failed_arg_index) != arguments->end());

    cl_region *free_index_mem = arguments->at(free_index_arg_index).first;
    cl_region *any_failed_mem = arguments->at(any_failed_arg_index).first;

    unsigned zero = 0;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, free_index_mem->sub_mem, CL_TRUE, 0,
                sizeof(zero), &zero, 0, NULL, NULL));

    cl_event fill_event;
    // CHECK(clEnqueueFillBuffer(dev_ctx->cmd, any_failed_mem, &zero, sizeof(zero),
    //             0, sizeof(unsigned), 0, NULL, &fill_event));
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, any_failed_mem->sub_mem, CL_FALSE, 0,
                sizeof(zero), &zero, 0, NULL, &fill_event));
    CHECK(clWaitForEvents(1, &fill_event));

    EXIT_TRACE("resetHeap");
}

#ifdef BRIDGE_DEBUG
static void save_to_dump_file(swat_context *context) {
    int dump_index = 0;
    int fd;
    do {
        char filename[256];
        sprintf(filename, "bridge.dump.%d", dump_index);
        fd = open(filename, O_WRONLY | O_CREAT, O_EXCL | S_IRUSR | S_IWUSR);
        dump_index++;
    } while(fd == -1);

    // Write kernel source to dump file
    safe_write(fd, &context->kernel_src_len, sizeof(context->kernel_src_len));
    safe_write(fd, context->kernel_src, context->kernel_src_len + 1);
    int num_args = context->debug_arguments->size();
    safe_write(fd, &num_args, sizeof(num_args));

    for (map<int, kernel_arg *>::iterator i =
            context->debug_arguments->begin(), e =
            context->debug_arguments->end(); i != e; i++) {
        int arg_index = i->first;
        kernel_arg *arg = i->second;

        safe_write(fd, &arg_index, sizeof(arg_index));
        arg->dump(fd);
    }

    close(fd);
}
#endif

JNI_JAVA(void, OpenCLBridge, run)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint range) {
    ENTER_TRACE("run");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    size_t global_range = range;
    cl_event event;

#ifdef BRIDGE_DEBUG
    save_to_dump_file(context);
#endif

#ifdef VERBOSE
    fprintf(stderr, "Host thread %d launching kernel on OpenCL device %s\n",
            context->host_thread_index, get_device_name(dev_ctx->dev));
    print_allocator(dev_ctx->allocator, context->host_thread_index);
#endif

    CHECK(clEnqueueNDRangeKernel(dev_ctx->cmd, context->kernel, 1, NULL,
                &global_range, NULL, 0, NULL, &event));
    CHECK(clWaitForEvents(1, &event));
    EXIT_TRACE("run");
}

#define LOAD_FROM_BB(type, capitalized_type) \
JNI_JAVA(int, OpenCLBridge, set##capitalized_type##ArrFromBB) \
        (JNIEnv *jenv, jclass clazz, long l_addressOfArr, jint bufferLength, jbyteArray bb, \
         jint position, jint remaining, jlong fieldOffset) { \
    assert(remaining % sizeof( type ) == 0); \
    const int remainingEles = remaining / sizeof( type ); \
    const unsigned to_process = (remainingEles > bufferLength ? bufferLength : remainingEles); \
    void **addressOfArr = (void **)l_addressOfArr; \
    \
    void *bb_elements = jenv->GetPrimitiveArrayCritical(bb, NULL); \
    type *bb_iter = ( type *)(((unsigned char *)bb_elements) + position); \
    \
    const unsigned chunking = 2; \
    const unsigned round_down = (to_process / chunking) * chunking; \
    for (unsigned i = 0; i < round_down; i += chunking) { \
        unsigned char * const ele1 = (unsigned char * const)addressOfArr[i]; \
        unsigned char * const ele2 = (unsigned char * const)addressOfArr[i + 1]; \
        \
        *(( type *)(ele1 + fieldOffset)) = *(bb_iter); \
        *(( type *)(ele2 + fieldOffset)) = *(bb_iter + 1); \
        \
        bb_iter += chunking; \
    } \
    \
    for (unsigned i = round_down; i < to_process; i++) { \
        unsigned char * const ele = (unsigned char * const)addressOfArr[i]; \
        *(( type *)(ele + fieldOffset)) = *(bb_iter); \
        bb_iter++; \
    } \
    \
    jenv->ReleasePrimitiveArrayCritical(bb, bb_elements, JNI_ABORT); \
    return to_process; \
}

LOAD_FROM_BB(int, Int)
LOAD_FROM_BB(float, Float)
LOAD_FROM_BB(double, Double)

JNI_JAVA(int, OpenCLBridge, setObjectArrFromBB)
        (JNIEnv *jenv, jclass clazz, long l_addressOfArr, jint bufferLength,
         jbyteArray bb, jint position, jint remaining, jintArray fieldSizes,
         jlongArray fieldOffsets, jint structSize) {
    assert(remaining % structSize == 0);
    unsigned nfields = jenv->GetArrayLength(fieldSizes);
    const int remainingEles = remaining / structSize;
    const unsigned to_process = (remainingEles > bufferLength ? bufferLength : remainingEles);
    void **addressOfArr = (void **)l_addressOfArr;

    unsigned char *bb_iter = (unsigned char *)jenv->GetPrimitiveArrayCritical(bb, NULL);
    int *sizes = (int *)jenv->GetPrimitiveArrayCritical(fieldSizes, NULL);
    long *offsets = (long *)jenv->GetPrimitiveArrayCritical(fieldOffsets, NULL);

    const unsigned chunking = 2;
    const unsigned round_down = (to_process / chunking) * chunking;
    for (unsigned i = 0; i < round_down; i+=chunking) {
        unsigned char * const ele1 = (unsigned char * const)addressOfArr[i];
        unsigned char * const ele2 = (unsigned char * const)addressOfArr[i + 1];

        unsigned char * bb_iter1 = bb_iter;
        unsigned char * bb_iter2 = bb_iter + structSize;

        for (unsigned j = 0; j < nfields; j++) {
            const int size = sizes[j];
            memcpy(ele1 + offsets[j], bb_iter1, size);
            memcpy(ele2 + offsets[j], bb_iter2, size);

            bb_iter1 += size;
            bb_iter2 += size;
        }
        bb_iter += (2 * structSize);
    }
    for (unsigned i = round_down; i < to_process; i++) {
        unsigned char * const ele1 = (unsigned char * const)addressOfArr[i];

        for (unsigned j = 0; j < nfields; j++) {
            const int size = sizes[j];
            memcpy(ele1 + offsets[j], bb_iter, size);
            bb_iter += size;
        }
    }

    jenv->ReleasePrimitiveArrayCritical(bb, bb_iter, JNI_ABORT);
    jenv->ReleasePrimitiveArrayCritical(fieldSizes, sizes, JNI_ABORT);
    jenv->ReleasePrimitiveArrayCritical(fieldOffsets, offsets, JNI_ABORT);
    return to_process;
}

JNI_JAVA(void, OpenCLBridge, writeToBBFromObjArray)
        (JNIEnv *jenv, jclass clazz, long l_addressOfArr, jint bufferLength,
         jbyteArray out, jint position, jintArray fieldSizes,
         jlongArray fieldOffsets) {
    void **addressOfArr = (void **)l_addressOfArr;
    const unsigned nfields = jenv->GetArrayLength(fieldSizes);

    unsigned char *bb_contents = (unsigned char *)jenv->GetPrimitiveArrayCritical(out, NULL);
    int *sizes = (int *)jenv->GetPrimitiveArrayCritical(fieldSizes, NULL);
    long *offsets = (long *)jenv->GetPrimitiveArrayCritical(fieldOffsets, NULL);

    unsigned char *bb_iter = bb_contents + position;

    for (unsigned i = 0; i < bufferLength; i++) {
        unsigned char * const ele = (unsigned char * const)addressOfArr[i];

        for (unsigned j = 0; j < nfields; j++) {
            const int size = sizes[j];
            memcpy(bb_iter, ele + offsets[j], size);
            bb_iter += size;
        }
    }

    jenv->ReleasePrimitiveArrayCritical(out, bb_contents, JNI_ABORT);
    jenv->ReleasePrimitiveArrayCritical(fieldSizes, sizes, JNI_ABORT);
    jenv->ReleasePrimitiveArrayCritical(fieldOffsets, offsets, JNI_ABORT);
}

#ifdef __cplusplus
}
#endif
