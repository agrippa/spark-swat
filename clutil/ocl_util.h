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
extern cl_uint get_device_pointer_size_in_bytes(cl_device_id device);

#endif
