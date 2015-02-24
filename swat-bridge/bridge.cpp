#include <jni.h>
#include <assert.h>
#include <string.h>

#include "bridge.h"

#define SET_ARRAY_ARG_MACRO(ltype, utype) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArrayArg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, j##ltype##Array arg) { \
    jsize len = jenv->GetArrayLength(arg) * sizeof(ltype); \
    ltype *arr = jenv->Get##utype##ArrayElements(arg, 0); \
    set_kernel_arg(arr, len, index, (swat_context *)lctx); \
    jenv->Release##utype##ArrayElements(arg, arr, 0); \
}

#ifdef __cplusplus
extern "C" {
#endif

static cl_uint get_num_opencl_platforms() {
    cl_uint num_platforms;
    CHECK(clGetPlatformIDs(0, NULL, &num_platforms));
    return num_platforms;
}

static cl_uint get_num_devices(cl_platform_id platform, cl_device_type type) {
    cl_uint num_devices;
    CHECK(clGetDeviceIDs(platform, type, 0, NULL, &num_devices));
    return num_devices;
}

static cl_uint get_num_gpus(cl_platform_id platform) {
    return get_num_devices(platform, CL_DEVICE_TYPE_GPU);
}

static cl_uint get_num_cpus(cl_platform_id platform) {
    return get_num_devices(platform, CL_DEVICE_TYPE_CPU);
}

static cl_uint get_num_compute_units(cl_device_id device) {
    cl_uint compute_units;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_MAX_COMPUTE_UNITS,
                sizeof(compute_units), &compute_units, NULL));
    return compute_units;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_rdd_cl_OpenCLBridge_createContext
        (JNIEnv *jenv, jclass clazz, jstring source) {
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
            CHECK(clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, 1, &device,
                        NULL));
            platform = platforms[platform_index];
            break;
        } else {
            cl_uint num_cpus = get_num_cpus(platforms[platform_index]);
            if (num_cpus > 0) {
                CHECK(clGetDeviceIDs(platform, CL_DEVICE_TYPE_CPU, 1,
                            &backup_device, NULL));
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

    free(platforms);

    cl_int err;
    cl_context_properties ctx_props[] = { CL_CONTEXT_PLATFORM,
        (cl_context_properties)platform, 0 };
    cl_context ctx = clCreateContext(ctx_props, 1, &device, NULL, NULL, &err);
    CHECK(err);

    cl_command_queue cmd = clCreateCommandQueue(ctx, device, 0, &err);
    CHECK(err);

    const char *raw_source = jenv->GetStringUTFChars(source, NULL);
    size_t source_size[] = { strlen(raw_source) };
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

    return (jlong)context;
}


JNIEXPORT void JNICALL Java_org_apache_spark_rdd_cl_OpenCLBridge_setIntArg
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jint arg) {
    swat_context *context = (swat_context *)lctx;
    CHECK(clSetKernelArg(context->kernel, index, sizeof(arg), &arg));
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
}

static void fetch_kernel_arg(void *host, size_t len, int index,
        swat_context *context) {
    assert(context->arguments->find(index) != context->arguments->end());
    cl_mem mem = (*context->arguments)[index];

    CHECK(clEnqueueReadBuffer(context->cmd, mem, CL_TRUE, 0, len, host,
                0, NULL, NULL));
}

SET_ARRAY_ARG_MACRO(int, Int)
SET_ARRAY_ARG_MACRO(double, Double)
// JNIEXPORT void JNICALL Java_org_apache_spark_rdd_cl_OpenCLBridge_setIntArrayArg
//         (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jintArray arg) {
//     jsize len = jenv->GetArrayLength(arg) * sizeof(int);
//     jint *arr = jenv->GetIntArrayElements(arg, 0);
//     set_kernel_arg(arr, len, index, (swat_context *)lctx);
//     jenv->ReleaseIntArrayElements(arg, arr, 0);
// }

JNIEXPORT void JNICALL Java_org_apache_spark_rdd_cl_OpenCLBridge_fetchIntArrayArg
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jintArray arg) {
    jsize len = jenv->GetArrayLength(arg) * sizeof(int);
    jint *arr = jenv->GetIntArrayElements(arg, 0);
    fetch_kernel_arg(arr, len, index, (swat_context *)lctx);
    jenv->ReleaseIntArrayElements(arg, arr, 0);
}

JNIEXPORT void JNICALL Java_org_apache_spark_rdd_cl_OpenCLBridge_run
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint range) {
    swat_context *context = (swat_context *)lctx;
    size_t global_range = range;
    cl_event event;

    CHECK(clEnqueueNDRangeKernel(context->cmd, context->kernel, 1, NULL,
                &global_range, NULL, 0, NULL, &event));
    CHECK(clWaitForEvents(1, &event));
}

#ifdef __cplusplus
}
#endif
