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
static pthread_mutex_t device_ctxs_lock = PTHREAD_MUTEX_INITIALIZER;
static int *virtual_devices = NULL;
static int n_virtual_devices = 0;

#define ARG_MACRO(ltype, utype) \
JNI_JAVA(void, OpenCLBridge, set##utype##Arg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, j##ltype arg) { \
    enter_trace("set"#utype"Arg"); \
    swat_context *context = (swat_context *)lctx; \
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(arg), &arg); \
    exit_trace("set"#utype"Arg"); \
}

#define SET_ARRAY_ARG_MACRO(ltype, utype, ctype) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArrayArg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, \
         j##ltype##Array arg, jint argLength, jlong broadcastId, jint rddid, \
         jint partitionid, jint offsetid, jint componentid) { \
    enter_trace("set"#utype"ArrayArg"); \
    jsize len = argLength * sizeof(ctype); \
    ctype *arr = jenv->Get##utype##ArrayElements(arg, 0); \
    device_context *dev_ctx = (device_context *)l_dev_ctx; \
    swat_context *context = (swat_context *)lctx; \
    int err = pthread_mutex_lock(&dev_ctx->lock); \
    assert(err == 0); \
    /* \
    if (broadcastId >= 0 && \
            dev_ctx->broadcast_cache->find(broadcastId) != \
                dev_ctx->broadcast_cache->end()) { \
        cl_region *region = dev_ctx->broadcast_cache->at(broadcastId); \
        cl_mem mem = region->sub_mem; \
        add_hold(region); \
        CHECK(clSetKernelArg(context->kernel, index, sizeof(mem), &mem)); \
        (*context->arguments)[index] = region; \
    } else if (rddid >= 0 && dev_ctx->rdd_cache->find(rdd_partition_offset( \
                    rddid, partitionid, offsetid, componentid)) != \
                dev_ctx->rdd_cache->end()) { \
        mem_and_size ms = dev_ctx->rdd_cache->at(rdd_partition_offset(rddid, \
                    partitionid, offsetid, componentid)); \
        assert(len == ms.get_size()); \
        cl_mem mem = ms.get_mem(); \
        CHECK(clSetKernelArg(context->kernel, index, sizeof(mem), &mem)); \
        (*context->arguments)[index] = mem_and_size(mem, len); \
    } else { \
    */ \
        set_kernel_arg(arr, len, index, context, dev_ctx, broadcastId, rddid); \
        /* \
        if (broadcastId >= 0) { \
            dev_ctx->broadcast_cache->insert(pair<jlong, cl_mem>(broadcastId, mem)); \
        } else if (rddid >= 0) { \
            dev_ctx->rdd_cache->insert( \
                    pair<rdd_partition_offset, mem_and_size>( \
                        rdd_partition_offset(rddid, partitionid, offsetid, \
                            componentid), mem_and_size(mem, len))); \
        } \
    } \
    */ \
    err = pthread_mutex_unlock(&dev_ctx->lock); \
    assert(err == 0); \
    jenv->Release##utype##ArrayElements(arg, arr, 0); \
    exit_trace("set"#utype"ArrayArg"); \
}

#define FETCH_ARRAY_ARG_MACRO(ltype, utype, ctype) \
JNI_JAVA(void, OpenCLBridge, fetch##utype##ArrayArg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, \
         j##ltype##Array arg, jint argLength, jlong broadcastId) { \
    enter_trace("fetch"#utype"ArrayArg"); \
    jsize len = argLength * sizeof(ctype); \
    ctype *arr = jenv->Get##utype##ArrayElements(arg, 0); \
    fetch_kernel_arg(arr, len, index, (swat_context *)lctx, (device_context *)l_dev_ctx); \
    jenv->Release##utype##ArrayElements(arg, arr, 0); \
    exit_trace("fetch"#utype"ArrayArg"); \
}

#define SET_PRIMITIVE_ARG_BY_NAME_MACRO(ltype, utype, desc) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArgByName) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jobject obj, \
         jstring name) { \
    enter_trace("set"#utype"ArgByName"); \
    swat_context *context = (swat_context *)lctx; \
    jclass enclosing_class = jenv->GetObjectClass(obj); \
    const char *raw_name = jenv->GetStringUTFChars(name, NULL); \
    jfieldID field = jenv->GetFieldID(enclosing_class, raw_name, desc); \
    jenv->ReleaseStringUTFChars(name, raw_name); \
    ltype val = jenv->Get##utype##Field(obj, field); \
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(val), &val); \
    exit_trace("set"#utype"ArgByName"); \
}

#define SET_ARRAY_ARG_BY_NAME_MACRO(ltype, utype, desc) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArrayArgByName) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, jobject obj, \
         jstring name) { \
    enter_trace("set"#utype"ArrayArgByName"); \
    jclass enclosing_class = jenv->GetObjectClass(obj); \
    const char *raw_name = jenv->GetStringUTFChars(name, NULL); \
    jfieldID field = jenv->GetFieldID(enclosing_class, raw_name, desc); \
    jenv->ReleaseStringUTFChars(name, raw_name); \
    jarray arr = (jarray)jenv->GetObjectField(obj, field); \
    jsize arr_length = jenv->GetArrayLength(arr) * sizeof(ltype); \
    ltype *arr_eles = jenv->Get##utype##ArrayElements((j##ltype##Array)arr, \
            0); \
    set_kernel_arg(arr_eles, arr_length, index, (swat_context *)lctx, \
            (device_context *)l_dev_ctx, -1, -1); \
    jenv->Release##utype##ArrayElements((j##ltype##Array)arr, arr_eles, 0); \
    exit_trace("set"#utype"ArrayArgByName"); \
}


#ifdef __cplusplus
extern "C" {
#endif

void enter_trace(const char *lbl) {
#ifdef TRACE
    fprintf(stderr, "entering %s\n", lbl);
#endif
}

void exit_trace(const char *lbl) {
#ifdef TRACE
    fprintf(stderr, "leaving %s\n", lbl);
#endif
}

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

                cl_command_queue cmd = clCreateCommandQueue(ctx, curr_dev, 0, &err);
                CHECK(err);

                device_ctxs[global_device_id].platform = platforms[platform_index];
                device_ctxs[global_device_id].dev = curr_dev;
                device_ctxs[global_device_id].ctx = ctx;
                device_ctxs[global_device_id].cmd = cmd;

                int perr = pthread_mutex_init(&(device_ctxs[global_device_id].lock), NULL);
                assert(perr == 0);

                device_ctxs[global_device_id].allocator = init_allocator(curr_dev, ctx);

                // device_ctxs[global_device_id].broadcast_cache =
                //     new map<jlong, cl_region *>();
                device_ctxs[global_device_id].program_cache =
                    new map<string, cl_program>();
                // device_ctxs[global_device_id].rdd_cache =
                //     new map<rdd_partition_offset, mem_and_size>();

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
    swat_context *ctx = (swat_context *)l_ctx;
    /*
    for (set<cl_mem>::iterator i = ctx->all_allocated->begin(),
            e = ctx->all_allocated->end(); i != e; i++) {
        cl_mem mem = *i;
#ifdef VERBOSE
        fprintf(stderr, "%d: Releasing mem object %p\n", ctx->host_thread_index, mem);
#endif
        CHECK(clReleaseMemObject(mem));
    }
    */

    // CHECK(clReleaseProgram(ctx->program));
    CHECK(clReleaseKernel(ctx->kernel));
    delete ctx->arguments;
    // delete ctx->all_allocated;
    free(ctx);
}

JNI_JAVA(jlong, OpenCLBridge, createSwatContext)
        (JNIEnv *jenv, jclass clazz, jstring label, jstring source,
         jlong l_dev_ctx, jint host_thread_index, jboolean requiresDouble,
         jboolean requiresHeap) {
    enter_trace("createContext");

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

        dev_ctx->program_cache->insert(pair<string, cl_program>(label_str,
                    program));
    }

    perr = pthread_mutex_unlock(&dev_ctx->lock);
    assert(perr == 0);

    cl_kernel kernel = clCreateKernel(program, "run", &err);
    CHECK(err);

    swat_context *context = (swat_context *)malloc(sizeof(swat_context));
    // context->program = program;
    context->kernel = kernel;
    context->host_thread_index = host_thread_index;
    context->arguments = new map<int, cl_region *>();
    // context->all_allocated = new set<cl_mem>();
#ifdef BRIDGE_DEBUG
    context->debug_arguments = new map<int, kernel_arg *>();
    context->kernel_src = store_source;
    context->kernel_src_len = source_len;
#endif

    exit_trace("createContext");
    return (jlong)context;
}

static cl_region *get_mem_cached(swat_context *context, device_context *dev_ctx,
        int index, size_t size, jlong broadcastId, jint rdd) {
    // if (context->arguments->find(index) != context->arguments->end() &&
    //         context->arguments->at(index).get_size() >= size) {
    //     return context->arguments->at(index).get_mem();
    // } else {
        cl_region *region = allocate_cl_region(size, dev_ctx->allocator);
        assert(region);

        // cl_mem mem = clCreateBuffer(dev_ctx->ctx, CL_MEM_READ_WRITE, size,
        //         NULL, &err);
#ifdef VERBOSE
        fprintf(stderr, "%d: Allocating %p, %lu bytes for index=%d\n",
                context->host_thread_index, region, size, index);
#endif

        // if (broadcastId < 0 && rdd < 0) {
        //     context->all_allocated->insert(mem);
        // }
        (*context->arguments)[index] = region;
        return region;
    // }
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
    cl_mem mem = context->arguments->at(index)->sub_mem;

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

    for (map<int, cl_region *>::iterator i = ctx->arguments->begin(),
            e = ctx->arguments->end(); i != e; i++) {
        cl_region *region = i->second;
        assert(region);

        free_cl_region(region, false);
    }
}

JNI_JAVA(void, OpenCLBridge, setArgUnitialized)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum,
         jlong size) {
    enter_trace("setArgUnitialized");
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

    exit_trace("setArgUnitialized");
}

JNI_JAVA(void, OpenCLBridge, setNullArrayArg)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint argnum) {
    enter_trace("setNullArrayArg");

    cl_mem none = 0x0;
    swat_context *context = (swat_context *)lctx;
    CHECK(clSetKernelArg(context->kernel, argnum, sizeof(none), &none));

    (*context->arguments)[argnum] = NULL;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[argnum] = new kernel_arg(&none, sizeof(none),
            false, false, 0);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a null value\n", argnum);
#endif

    exit_trace("setNullArrayArg");
}


JNI_JAVA(int, OpenCLBridge, createHeap)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum, jint size,
         jint max_n_buffered) {
    enter_trace("createHeap");

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
    fprintf(stderr, "%d: Allocating %p, %lu bytes for processing_succeeded\n",
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
    fprintf(stderr, "%d: Allocating %p, %lu bytes for any_failed\n",
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

    exit_trace("createHeap");
    return (local_argnum - argnum); // Number of kernel arguments added
}

JNI_JAVA(void, OpenCLBridge, resetHeap)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint starting_argnum) {
    enter_trace("resetHeap");

    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    map<int, cl_region *> *arguments = context->arguments;
    int free_index_arg_index = starting_argnum + 1;
    int any_failed_arg_index = starting_argnum + 4;

    assert(arguments->find(free_index_arg_index) != arguments->end());
    assert(arguments->find(any_failed_arg_index) != arguments->end());

    cl_region *free_index_mem = arguments->at(free_index_arg_index);
    cl_region *any_failed_mem = arguments->at(any_failed_arg_index);

    unsigned zero = 0;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, free_index_mem->sub_mem, CL_TRUE, 0,
                sizeof(zero), &zero, 0, NULL, NULL));

    cl_event fill_event;
    // CHECK(clEnqueueFillBuffer(dev_ctx->cmd, any_failed_mem, &zero, sizeof(zero),
    //             0, sizeof(unsigned), 0, NULL, &fill_event));
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, any_failed_mem->sub_mem, CL_FALSE, 0,
                sizeof(zero), &zero, 0, NULL, &fill_event));
    CHECK(clWaitForEvents(1, &fill_event));

    exit_trace("resetHeap");
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
    enter_trace("run");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    size_t global_range = range;
    cl_event event;

#ifdef BRIDGE_DEBUG
    save_to_dump_file(context);
#endif

    CHECK(clEnqueueNDRangeKernel(dev_ctx->cmd, context->kernel, 1, NULL,
                &global_range, NULL, 0, NULL, &event));
    CHECK(clWaitForEvents(1, &event));
    exit_trace("run");
}

#ifdef __cplusplus
}
#endif
