#include <jni.h>
#include <assert.h>
#include <string.h>

#include "bridge.h"

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

JNI_JAVA(jlong, OpenCLBridge, createContext)
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

    cl_kernel kernel = clCreateKernel(program, "run", &err);
    CHECK(err);

    swat_context *context = (swat_context *)malloc(sizeof(swat_context));
    context->platform = platform;
    context->device = device;
    context->ctx = ctx;
    context->program = program;
    context->kernel = kernel;
    context->cmd = cmd;

    return (jlong)context;
}
