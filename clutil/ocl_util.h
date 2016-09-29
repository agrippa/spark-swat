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


#ifndef OCL_UTIL_H
#define OCL_UTIL_H

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
typedef uint32_t cl_uint;
typedef int32_t cl_int;
typedef int cl_platform_id;
typedef CUdevice cl_device_id;
typedef enum {
    CL_DEVICE_TYPE_CPU,
    CL_DEVICE_TYPE_GPU,
    CL_DEVICE_TYPE_ALL
} cl_device_type;
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

#ifdef USE_CUDA
#define CHECK(call) { \
    const cudaError_t __err = (call); \
    if (__err != cudaSuccess) { \
        fprintf(stderr, "CUDA Runtime Error at %s:%d - %s\n", __FILE__, \
                __LINE__, cudaGetErrorString(__err)); \
        exit(1); \
    } \
}

#define CHECK_DRIVER(call) { \
    const CUresult _err = (call); \
    if (_err != CUDA_SUCCESS) { \
        fprintf(stderr, "CUDA Driver Error at %s:%d - %d\n", __FILE__, \
                __LINE__, _err); \
        exit(1); \
    } \
}
#else
#define CHECK(call) { \
    const cl_int __err = (call); \
    if (__err != CL_SUCCESS) { \
        fprintf(stderr, "OpenCL Error at %s:%d - %d\n", \
                __FILE__, __LINE__, __err); \
        exit(1); \
    } \
}
#endif

extern cl_uint get_num_platforms();
extern cl_uint get_num_devices(cl_platform_id platform, cl_device_type type);
extern cl_uint get_num_gpus(cl_platform_id platform);
extern cl_uint get_num_cpus(cl_platform_id platform);
extern cl_uint get_num_devices(cl_platform_id platform);
extern char *get_device_name(cl_device_id device);
extern const char *get_device_type_str(cl_device_id device);
extern cl_device_type get_device_type(cl_device_id device);
extern cl_uint get_total_num_devices();
extern cl_uint get_device_pointer_size_in_bytes(cl_device_id device);

extern void get_platform_ids(cl_platform_id *platforms, const unsigned capacity);
extern void get_device_ids(cl_platform_id platform, cl_device_id *devices,
        const unsigned capacity);

#endif
