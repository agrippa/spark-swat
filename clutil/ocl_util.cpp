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

#include "ocl_util.h"
#include <assert.h>
#include <string.h>

cl_uint get_num_platforms() {
#ifdef USE_CUDA
    // Emulate the idea of OCL platforms by putting all GPUs in a single platform
    return 1;
#else
    cl_uint num_platforms;
    CHECK(clGetPlatformIDs(0, NULL, &num_platforms));
    return num_platforms;
#endif
}

cl_uint get_num_devices(cl_platform_id platform, cl_device_type type) {
#ifdef USE_CUDA
    assert(platform == 0);
    if (type == CL_DEVICE_TYPE_GPU || type == CL_DEVICE_TYPE_ALL) {
        int count;
        CHECK_DRIVER(cuDeviceGetCount(&count));
        return count;
    } else {
        return 0;
    }
#else
    cl_uint num_devices;
    cl_int err = clGetDeviceIDs(platform, type, 0, NULL, &num_devices);
    if (err == CL_DEVICE_NOT_FOUND) {
        return 0;
    } else {
        CHECK(err);
    }
    return num_devices;
#endif
}

cl_uint get_num_gpus(cl_platform_id platform) {
    return get_num_devices(platform, CL_DEVICE_TYPE_GPU);
}

cl_uint get_num_cpus(cl_platform_id platform) {
    return get_num_devices(platform, CL_DEVICE_TYPE_CPU);
}

cl_uint get_num_devices(cl_platform_id platform) {
    return get_num_devices(platform, CL_DEVICE_TYPE_ALL);
}

cl_uint get_total_num_devices() {
#ifdef USE_CUDA
    return get_num_devices(0, CL_DEVICE_TYPE_GPU);
#else
    cl_uint count_devices = 0;
    cl_uint num_platforms = get_num_platforms();

    cl_platform_id *platforms =
        (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
    CHECK_ALLOC(platforms);
    CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));

    for (cl_uint platform_index = 0; platform_index < num_platforms;
            platform_index++) {
        count_devices += get_num_devices(platforms[platform_index]);
    }

    free(platforms);

    return count_devices;
#endif
}

char *get_device_name(cl_device_id device) {
#ifdef USE_CUDA
    char name_buf[1024];
    CHECK_DRIVER(cuDeviceGetName(name_buf, 1024, device));
    char *device_name = (char *)malloc(strlen(name_buf) + 1);
    memcpy(device_name, name_buf, strlen(name_buf) + 1);
    return device_name;
#else
    size_t name_len;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_NAME, 0, NULL, &name_len));
    char *device_name = (char *)malloc(name_len + 1);
    CHECK_ALLOC(device_name);
    CHECK(clGetDeviceInfo(device, CL_DEVICE_NAME, name_len, device_name,
                NULL));
    device_name[name_len] = '\0';
    return device_name;
#endif
}

const char *get_device_type_str(cl_device_id device) {
    cl_device_type type = get_device_type(device);
    switch (type) {
        case (CL_DEVICE_TYPE_GPU):
            return "GPU";
        case (CL_DEVICE_TYPE_CPU):
            return "CPU";
        default:
            fprintf(stderr, "Unsupported device type %d\n", (int)type);
            exit(1);
    }
}

cl_device_type get_device_type(cl_device_id device) {
#ifdef USE_CUDA
    return CL_DEVICE_TYPE_GPU;
#else
    cl_device_type type;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_TYPE, sizeof(type), &type, NULL));
    return type;
#endif
}

cl_uint get_num_compute_units(cl_device_id device) {
#ifdef USE_CUDA
    int compute_units;
    CHECK_DRIVER(cuDeviceGetAttribute(&compute_units,
                CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT, device));
    return compute_units;
#else
    cl_uint compute_units;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_MAX_COMPUTE_UNITS,
                sizeof(compute_units), &compute_units, NULL));
    return compute_units;
#endif
}

cl_uint get_device_pointer_size_in_bytes(cl_device_id device) {
#ifdef USE_CUDA
    return 8; // TODO is this safe?
#else
    cl_uint pointer_size_in_bits;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_ADDRESS_BITS,
                sizeof(pointer_size_in_bits), &pointer_size_in_bits, NULL));
    assert(pointer_size_in_bits % 8 == 0);
    return pointer_size_in_bits / 8;
#endif
}

void get_platform_ids(cl_platform_id *platforms, const unsigned capacity) {
#ifdef USE_CUDA
    assert(capacity == 1);
    *platforms = 0;
#else
    CHECK(clGetPlatformIDs(capacity, platforms, NULL)); 
#endif
}

void get_device_ids(cl_platform_id platform, cl_device_id *devices,
        const unsigned capacity) {
#ifdef USE_CUDA
    assert(platform == 0);
    const unsigned count = get_num_devices(platform, CL_DEVICE_TYPE_ALL);
    assert(capacity >= count);
    for (unsigned i = 0; i < count; i++) {
        CHECK_DRIVER(cuDeviceGet(devices + i, i));
    }
#else
    CHECK(clGetDeviceIDs(platform, CL_DEVICE_TYPE_ALL,
                capacity, devices, NULL)); 
#endif
}
