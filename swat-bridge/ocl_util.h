#ifndef OCL_UTIL_H
#define OCL_UTIL_H

#include <stdio.h>

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#define CHECK(call) { \
    cl_int __err = (call); \
    if (__err != CL_SUCCESS) { \
        fprintf(stderr, "OpenCL Error at %s:%d - %d\n", __FILE__, __LINE__, \
                __err); \
        exit(1); \
    } \
}

extern cl_uint get_num_opencl_platforms();
extern cl_uint get_num_devices(cl_platform_id platform, cl_device_type type);
extern cl_uint get_num_gpus(cl_platform_id platform);
extern cl_uint get_num_cpus(cl_platform_id platform);
extern cl_uint get_num_devices(cl_platform_id platform);

#endif
