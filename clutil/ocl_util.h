#ifndef OCL_UTIL_H
#define OCL_UTIL_H

#include <stdlib.h>
#include <stdio.h>

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#define CHECK_JNI(something) { \
    if ((something) == NULL) { \
        fprintf(stderr, "Some JNI operation failed at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
}

#define CHECK_ALLOC(ptr) { \
    if ((ptr) == NULL) { \
        fprintf(stderr, "Allocation failed at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
}

#define CHECK(call) { \
    const cl_int __err = (call); \
    if (__err != CL_SUCCESS) { \
        fprintf(stderr, "OpenCL Error at %s:%d - %d\n", \
                __FILE__, __LINE__, __err); \
        exit(1); \
    } \
}

extern cl_uint get_num_opencl_platforms();
extern cl_uint get_num_devices(cl_platform_id platform, cl_device_type type);
extern cl_uint get_num_gpus(cl_platform_id platform);
extern cl_uint get_num_cpus(cl_platform_id platform);
extern cl_uint get_num_devices(cl_platform_id platform);
extern char *get_device_name(cl_device_id device);
extern const char *get_device_type_str(cl_device_id device);
extern cl_device_type get_device_type(cl_device_id device);
extern cl_uint get_total_num_devices();

#endif
