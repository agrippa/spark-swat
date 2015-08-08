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
         j##ltype##Array arg, jint argLength, jlong broadcastId) { \
    enter_trace("set"#utype"ArrayArg"); \
    jsize len = argLength * sizeof(ctype); \
    ctype *arr = jenv->Get##utype##ArrayElements(arg, 0); \
    device_context *dev_ctx = (device_context *)l_dev_ctx; \
    swat_context *context = (swat_context *)lctx; \
    int err = pthread_mutex_lock(&dev_ctx->lock); \
    assert(err == 0); \
    if (broadcastId >= 0 && \
            dev_ctx->broadcast_cache->find(broadcastId) != \
                dev_ctx->broadcast_cache->end()) { \
        fprintf(stderr, "SWAT Using cached copy for broadcast id %ld\n", broadcastId); \
        cl_mem mem = dev_ctx->broadcast_cache->at(broadcastId); \
        CHECK(clSetKernelArg(context->kernel, index, sizeof(mem), &mem)); \
        (*context->arguments)[index] = mem; \
    } else { \
        cl_mem mem = set_kernel_arg(arr, len, index, context, dev_ctx); \
        dev_ctx->broadcast_cache->insert(pair<jlong, cl_mem>(broadcastId, mem)); \
    } \
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
    set_kernel_arg(arr_eles, arr_length, index, (swat_context *)lctx, (device_context *)l_dev_ctx); \
    jenv->Release##utype##ArrayElements((j##ltype##Array)arr, arr_eles, 0); \
    exit_trace("set"#utype"ArrayArgByName"); \
}


#ifdef __cplusplus
extern "C" {
#endif

inline void enter_trace(const char *lbl) {
#ifdef TRACE
    fprintf(stderr, "entering %s\n", lbl);
#endif
}

inline void exit_trace(const char *lbl) {
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

static cl_uint get_num_compute_units(cl_device_id device) {
    cl_uint compute_units;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_MAX_COMPUTE_UNITS,
                sizeof(compute_units), &compute_units, NULL));
    return compute_units;
}

static int checkExtension(char *exts, size_t ext_len, const char *ext) {
    int start = 0;
    while (start < ext_len) {
        int end = start;
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
static int *parseDeviceConfig(int *ndevices_out) {
    assert(ndevices_out);

    char *device_config_str = getenv("SWAT_DEVICE_CONFIG");
    if (device_config_str == NULL) {
        fprintf(stderr, "Error: SWAT_DEVICE_CONFIG must be set in spark-env.sh\n");
        exit(1);
    }

    int device_config_str_len = strlen(device_config_str);
    int ncommas = 0;
    for (int i = 0; i < device_config_str_len; i++) {
        if (device_config_str[i] == ',') ncommas++;
    }
    int ndevices = ncommas + 1;
    int *devices = (int *)malloc(ndevices * sizeof(int));

    int start_index = 0;
    int end_index = 1;
    int count_devices = 0;
    while (end_index <= device_config_str_len) { // assume null terminator
        if (device_config_str[end_index] == ',' ||
                device_config_str[end_index] == '\0') {
            device_config_str[end_index] = '\0';
            int this_device_id = atoi(device_config_str + start_index);
            devices[count_devices++] = this_device_id;

            end_index++;
            start_index = end_index;
        } else {
            end_index++;
        }
    }
    assert(count_devices == ndevices);

    *ndevices_out = ndevices;
    return devices;
}

JNI_JAVA(jlong, OpenCLBridge, getDeviceContext)
        (JNIEnv *jenv, jclass clazz, int host_thread_index) {
    int ndevices;
    int *devices = parseDeviceConfig(&ndevices);
    int target_device = devices[host_thread_index % ndevices];
    free(devices);

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

        int global_device_id = 0;
        for (int platform_index = 0; platform_index < num_platforms; platform_index++) {
            cl_uint num_devices = get_num_devices(platforms[platform_index]);
            cl_device_id *devices = (cl_device_id *)malloc(num_devices * sizeof(cl_device_id));
            CHECK(clGetDeviceIDs(platforms[platform_index], CL_DEVICE_TYPE_ALL,
                        num_devices, devices, NULL));

            for (int i = 0; i < num_devices; i++) {
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
                device_ctxs[global_device_id].broadcast_cache = new map<jlong, cl_mem>();

                int perr = pthread_mutex_init(&(device_ctxs[global_device_id].lock), NULL);
                assert(perr == 0);

                global_device_id++;
            }

            free(devices);
        }

    }

    device_context *my_ctx = device_ctxs + target_device;

    perr = pthread_mutex_unlock(&device_ctxs_lock);
    assert(perr == 0);

    return (jlong)my_ctx;
}

JNI_JAVA(jlong, OpenCLBridge, createSwatContext)
        (JNIEnv *jenv, jclass clazz, jstring source, jlong l_dev_ctx,
         jboolean requiresDouble, jboolean requiresHeap) {
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

    const char *raw_source = jenv->GetStringUTFChars(source, NULL);
    size_t source_len = strlen(raw_source);
#ifdef BRIDGE_DEBUG
    char *store_source = (char *)malloc(source_len + 1);
    memcpy(store_source, raw_source, source_len);
    store_source[source_len] = '\0';
#endif

    cl_int err;
    size_t source_size[] = { source_len };
    cl_program program = clCreateProgramWithSource(dev_ctx->ctx, 1, &raw_source,
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

    cl_kernel kernel = clCreateKernel(program, "run", &err);
    CHECK(err);

    swat_context *context = (swat_context *)malloc(sizeof(swat_context));
    context->program = program;
    context->kernel = kernel;
    context->arguments = new map<int, cl_mem>();
#ifdef BRIDGE_DEBUG
    context->debug_arguments = new map<int, kernel_arg *>();
    context->kernel_src = store_source;
    context->kernel_src_len = source_len;
#endif

    exit_trace("createContext");
    return (jlong)context;
}

static cl_mem set_kernel_arg(void *host, size_t len, int index,
        swat_context *context, device_context *dev_ctx) {
    cl_int err;
    cl_mem mem = clCreateBuffer(dev_ctx->ctx, CL_MEM_READ_WRITE, len,
            NULL, &err);
    CHECK(err);

    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, mem, CL_TRUE, 0, len, host,
                0, NULL, NULL));

    CHECK(clSetKernelArg(context->kernel, index, sizeof(mem), &mem));

    (*context->arguments)[index] = mem;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[index] = new kernel_arg(host, len, true, false,
            0);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a memory buffer of size %lu\n", index,
            len);
#endif
    return mem;
}

static void fetch_kernel_arg(void *host, size_t len, int index,
        swat_context *context, device_context *dev_ctx) {
    assert(context->arguments->find(index) != context->arguments->end());
    cl_mem mem = context->arguments->at(index);

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

JNI_JAVA(void, OpenCLBridge, setArgUnitialized)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum, jlong size) {
    enter_trace("setArgUnitialized");
    cl_int err;
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;

    cl_mem mem = clCreateBuffer(dev_ctx->ctx, CL_MEM_READ_WRITE, size, NULL,
            &err);
    CHECK(err);
    CHECK(clSetKernelArg(context->kernel, argnum, sizeof(mem), &mem));

    (*context->arguments)[argnum] = mem;

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

    cl_int err;
    cl_mem none = 0x0;
    swat_context *context = (swat_context *)lctx;
    CHECK(clSetKernelArg(context->kernel, argnum, sizeof(none), &none));

    (*context->arguments)[argnum] = none;

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

    cl_int err;
    unsigned zero = 0;
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    jint local_argnum = argnum;

    // The heap
    cl_mem mem = clCreateBuffer(dev_ctx->ctx, CL_MEM_READ_WRITE, size,
            NULL, &err);
    CHECK(err);
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(mem), &mem));
    (*context->arguments)[local_argnum++] = mem;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL, size,
            true, false, 0);
#endif

    // free_index
    mem = clCreateBuffer(dev_ctx->ctx, CL_MEM_READ_WRITE, sizeof(zero), NULL,
            &err);
    CHECK(err);
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, mem, CL_TRUE, 0, sizeof(zero),
                &zero, 0, NULL, NULL));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(mem), &mem));
    (*context->arguments)[local_argnum++] = mem;

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
    mem = clCreateBuffer(dev_ctx->ctx, CL_MEM_READ_WRITE,
            sizeof(int) * max_n_buffered, NULL, &err);
    CHECK(err);
    cl_event fill_event;
    CHECK(clEnqueueFillBuffer(dev_ctx->cmd, mem, &zero, sizeof(zero), 0,
                sizeof(int) * max_n_buffered, 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(mem), &mem));
    CHECK(clWaitForEvents(1, &fill_event));
    (*context->arguments)[local_argnum++] = mem;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(int) * max_n_buffered, true, true, zero);
#endif

    // any_failed
    mem = clCreateBuffer(dev_ctx->ctx, CL_MEM_READ_WRITE, sizeof(unsigned),
            NULL, &err);
    CHECK(err);
    CHECK(clEnqueueFillBuffer(dev_ctx->cmd, mem, &zero, sizeof(zero), 0,
                sizeof(unsigned), 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(mem), &mem));
    CHECK(clWaitForEvents(1, &fill_event));
    (*context->arguments)[local_argnum++] = mem;

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
    map<int, cl_mem> *arguments = context->arguments;
    int free_index_arg_index = starting_argnum + 1;
    int any_failed_arg_index = starting_argnum + 4;

    assert(arguments->find(free_index_arg_index) != arguments->end());
    assert(arguments->find(any_failed_arg_index) != arguments->end());

    cl_mem free_index_mem = (*arguments)[free_index_arg_index];
    cl_mem any_failed_mem = (*arguments)[any_failed_arg_index];

    unsigned zero = 0;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, free_index_mem, CL_TRUE, 0,
                sizeof(zero), &zero, 0, NULL, NULL));

    cl_event fill_event;
    CHECK(clEnqueueFillBuffer(dev_ctx->cmd, any_failed_mem, &zero, sizeof(zero),
                0, sizeof(unsigned), 0, NULL, &fill_event));
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
