#include <jni.h>
#include <assert.h>
#include <string.h>

#include "bridge.h"
#include "common.h"
#include "kernel_arg.h"
#include "ocl_util.h"

#define ARG_MACRO(ltype, utype) \
JNI_JAVA(void, OpenCLBridge, set##utype##Arg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, j##ltype arg) { \
    enter_trace("set##utype##Arg"); \
    swat_context *context = (swat_context *)lctx; \
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(arg), &arg); \
    exit_trace("set##utype##Arg"); \
}

#define ARRAY_ARG_MACRO(ltype, utype, ctype, type) \
JNI_JAVA(void, OpenCLBridge, type##utype##ArrayArg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, j##ltype##Array arg) { \
    enter_trace("type##utype##ArrayArg"); \
    jsize len = jenv->GetArrayLength(arg) * sizeof(ctype); \
    ctype *arr = jenv->Get##utype##ArrayElements(arg, 0); \
    type##_kernel_arg(arr, len, index, (swat_context *)lctx); \
    jenv->Release##utype##ArrayElements(arg, arr, 0); \
    exit_trace("type##utype##ArrayArg"); \
}

#define SET_ARRAY_ARG_MACRO(ltype, utype, ctype) ARRAY_ARG_MACRO(ltype, utype, ctype, set)
#define FETCH_ARRAY_ARG_MACRO(ltype, utype, ctype) ARRAY_ARG_MACRO(ltype, utype, ctype, fetch)

#define SET_PRIMITIVE_ARG_BY_NAME_MACRO(ltype, utype, desc) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArgByName) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jobject obj, \
         jstring name) { \
    enter_trace("set##utype##ArgByName"); \
    swat_context *context = (swat_context *)lctx; \
    jclass enclosing_class = jenv->GetObjectClass(obj); \
    const char *raw_name = jenv->GetStringUTFChars(name, NULL); \
    jfieldID field = jenv->GetFieldID(enclosing_class, raw_name, desc); \
    jenv->ReleaseStringUTFChars(name, raw_name); \
    ltype val = jenv->Get##utype##Field(obj, field); \
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(val), &val); \
    exit_trace("set##utype##ArgByName"); \
}

#define SET_ARRAY_ARG_BY_NAME_MACRO(ltype, utype, desc) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArrayArgByName) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jobject obj, \
         jstring name) { \
    enter_trace("set##utype##ArrayArgByName"); \
    jclass enclosing_class = jenv->GetObjectClass(obj); \
    const char *raw_name = jenv->GetStringUTFChars(name, NULL); \
    jfieldID field = jenv->GetFieldID(enclosing_class, raw_name, desc); \
    jenv->ReleaseStringUTFChars(name, raw_name); \
    jarray arr = (jarray)jenv->GetObjectField(obj, field); \
    jsize arr_length = jenv->GetArrayLength(arr) * sizeof(ltype); \
    ltype *arr_eles = jenv->Get##utype##ArrayElements((j##ltype##Array)arr, \
            0); \
    set_kernel_arg(arr_eles, arr_length, index, (swat_context *)lctx); \
    jenv->Release##utype##ArrayElements((j##ltype##Array)arr, arr_eles, 0); \
    exit_trace("set##utype##ArrayArgByName"); \
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

JNI_JAVA(jlong, OpenCLBridge, createContext)
        (JNIEnv *jenv, jclass clazz, jstring source, jboolean requiresDouble,
         jboolean requiresAtomics) {
    enter_trace("createContext");
    cl_uint num_platforms = get_num_opencl_platforms();
    cl_platform_id *platforms =
        (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
    CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));

    cl_platform_id backup_platform = 0;
    cl_device_id backup_device = 0;

    cl_platform_id platform = 0;
    cl_device_id device = 0;

    cl_uint platform_index = 0;
    while (platform_index < num_platforms && device == 0) {
        cl_uint num_gpus = get_num_gpus(platforms[platform_index]);
        if (num_gpus > 0) {
            CHECK(clGetDeviceIDs(platforms[platform_index], CL_DEVICE_TYPE_GPU,
                        1, &device, NULL));
            platform = platforms[platform_index];
            break;
        } else {
            cl_uint num_cpus = get_num_cpus(platforms[platform_index]);
            if (num_cpus > 0) {
                CHECK(clGetDeviceIDs(platforms[platform_index],
                            CL_DEVICE_TYPE_CPU, 1, &backup_device, NULL));
                backup_platform = platforms[platform_index];
            }
        }

        platform_index++;
    }

    if (device == 0) {
        device = backup_device;
        platform = backup_platform;
    }
    assert(device != 0);

    int requires_extension_check = (requiresDouble || requiresAtomics);
    if (requires_extension_check) {
        size_t ext_len;
        CHECK(clGetDeviceInfo(device, CL_DEVICE_EXTENSIONS, 0, NULL, &ext_len));
        char *exts = (char *)malloc(ext_len);
        CHECK(clGetDeviceInfo(device, CL_DEVICE_EXTENSIONS, ext_len, exts, NULL));

        if (requiresDouble) {
            checkExtension(exts, ext_len, "cl_khr_fp64");
        }
        if (requiresAtomics) {
            checkExtension(exts, ext_len, "cl_khr_int64_base_atomics");
            checkExtension(exts, ext_len, "cl_khr_int64_extended_atomics");
        }
    }

    free(platforms);

    cl_int err;
    cl_context_properties ctx_props[] = { CL_CONTEXT_PLATFORM,
        (cl_context_properties)platform, 0 };
    cl_context ctx = clCreateContext(ctx_props, 1, &device, NULL, NULL, &err);
    CHECK(err);

    cl_command_queue cmd = clCreateCommandQueue(ctx, device, 0, &err);
    CHECK(err);

    const char *raw_source = jenv->GetStringUTFChars(source, NULL);
    size_t source_len = strlen(raw_source);
#ifdef BRIDGE_DEBUG
    char *store_source = (char *)malloc(source_len + 1);
    memcpy(store_source, raw_source, source_len);
    store_source[source_len] = '\0';
#endif
    size_t source_size[] = { source_len };
    cl_program program = clCreateProgramWithSource(ctx, 1, &raw_source,
            source_size, &err);
    jenv->ReleaseStringUTFChars(source, raw_source);
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
        fprintf(stderr, "Build failure:\n%s\n", build_log);
        free(build_log);
    }
    CHECK(err);

    cl_kernel kernel = clCreateKernel(program, "run", &err);
    CHECK(err);

    swat_context *context = (swat_context *)malloc(sizeof(swat_context));
    context->platform = platform;
    context->device = device;
    context->ctx = ctx;
    context->program = program;
    context->kernel = kernel;
    context->cmd = cmd;
    context->arguments = new map<int, cl_mem>();
#ifdef BRIDGE_DEBUG
    context->debug_arguments = new map<int, kernel_arg *>();
    context->kernel_src = store_source;
    context->kernel_src_len = source_len;
#endif

    exit_trace("createContext");
    return (jlong)context;
}

static void set_kernel_arg(void *host, size_t len, int index,
        swat_context *context) {
    cl_int err;
    cl_mem mem = clCreateBuffer(context->ctx, CL_MEM_READ_WRITE, len,
            NULL, &err);
    CHECK(err);

    CHECK(clEnqueueWriteBuffer(context->cmd, mem, CL_TRUE, 0, len, host,
                0, NULL, NULL));

    CHECK(clSetKernelArg(context->kernel, index, sizeof(mem), &mem));

    (*context->arguments)[index] = mem;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[index] = new kernel_arg(host, len, true, false,
            0);
#endif
}

static void fetch_kernel_arg(void *host, size_t len, int index,
        swat_context *context) {
    assert(context->arguments->find(index) != context->arguments->end());
    cl_mem mem = (*context->arguments)[index];

    CHECK(clEnqueueReadBuffer(context->cmd, mem, CL_TRUE, 0, len, host,
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

JNI_JAVA(int, OpenCLBridge, createHeap)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint argnum, jlong size,
         jint max_n_buffered) {
    enter_trace("createHeap");

    cl_int err;
    unsigned zero = 0;
    swat_context *context = (swat_context *)lctx;
    jint local_argnum = argnum;

    // The heap
    cl_mem mem = clCreateBuffer(context->ctx, CL_MEM_READ_WRITE, size,
            NULL, &err);
    CHECK(err);
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(mem), &mem));
    (*context->arguments)[local_argnum++] = mem;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL, size,
            true, false, 0);
#endif

    // free_index
    mem = clCreateBuffer(context->ctx, CL_MEM_READ_WRITE, sizeof(zero), NULL,
            &err);
    CHECK(err);
    CHECK(clEnqueueWriteBuffer(context->cmd, mem, CL_TRUE, 0, sizeof(zero),
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
    mem = clCreateBuffer(context->ctx, CL_MEM_READ_WRITE,
            sizeof(int) * max_n_buffered, NULL, &err);
    CHECK(err);
    cl_event fill_event;
    CHECK(clEnqueueFillBuffer(context->cmd, mem, &zero, sizeof(zero), 0,
                sizeof(int) * max_n_buffered, 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(mem), &mem));
    CHECK(clWaitForEvents(1, &fill_event));
    (*context->arguments)[local_argnum++] = mem;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(int) * max_n_buffered, true, true, zero);
#endif

    // any_failed
    mem = clCreateBuffer(context->ctx, CL_MEM_READ_WRITE, sizeof(unsigned),
            NULL, &err);
    CHECK(err);
    CHECK(clEnqueueFillBuffer(context->cmd, mem, &zero, sizeof(zero), 0,
                sizeof(unsigned), 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(mem), &mem));
    CHECK(clWaitForEvents(1, &fill_event));
    (*context->arguments)[local_argnum++] = mem;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(unsigned), true, true, zero);
#endif

    exit_trace("createHeap");
    return (local_argnum - argnum); // Number of kernel arguments added
}

JNI_JAVA(void, OpenCLBridge, resetHeap)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint starting_argnum) {
    enter_trace("resetHeap");

    swat_context *context = (swat_context *)lctx;
    map<int, cl_mem> *arguments = context->arguments;
    int free_index_arg_index = starting_argnum + 1;
    int any_failed_arg_index = starting_argnum + 4;

    assert(arguments->find(free_index_arg_index) != arguments->end());
    assert(arguments->find(any_failed_arg_index) != arguments->end());

    cl_mem free_index_mem = (*arguments)[free_index_arg_index];
    cl_mem any_failed_mem = (*arguments)[any_failed_arg_index];

    unsigned zero = 0;
    CHECK(clEnqueueWriteBuffer(context->cmd, free_index_mem, CL_TRUE, 0,
                sizeof(zero), &zero, 0, NULL, NULL));

    cl_event fill_event;
    CHECK(clEnqueueFillBuffer(context->cmd, any_failed_mem, &zero, sizeof(zero),
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
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint range) {
    enter_trace("run");
    swat_context *context = (swat_context *)lctx;
    size_t global_range = range;
    cl_event event;

#ifdef BRIDGE_DEBUG
    save_to_dump_file(context);
#endif

    CHECK(clEnqueueNDRangeKernel(context->cmd, context->kernel, 1, NULL,
                &global_range, NULL, 0, NULL, &event));
    CHECK(clWaitForEvents(1, &event));
    exit_trace("run");
}

#ifdef __cplusplus
}
#endif
