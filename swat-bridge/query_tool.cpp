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

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <map>

#ifdef USE_CUDA
#else
#include <CL/cl.h>
#endif

#include "ocl_util.h"

void list_devices() {
    cl_uint num_platforms = get_num_platforms();
    cl_platform_id *platforms =
        (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
    get_platform_ids(platforms, num_platforms);
    int device_index = 0;

    for (cl_uint platform_index = 0; platform_index < num_platforms;
            platform_index++) {
        cl_uint num_devices = get_num_devices(platforms[platform_index]);

        printf("Platform %d (%d devices)\n", platform_index, num_devices);

        cl_device_id *devices = (cl_device_id *)malloc(sizeof(cl_device_id) *
                num_devices);
        get_device_ids(platforms[platform_index], devices, num_devices);
        for (unsigned d = 0; d < num_devices; d++) {
            cl_device_id dev = devices[d];
            printf("  Device %d - %s - %s\n", device_index,
                    get_device_type_str(dev), get_device_name(dev));

#ifndef USE_CUDA
            size_t version_len;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_VERSION, 0, NULL, &version_len));
            char *device_version = (char *)malloc(version_len + 1);
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_VERSION, version_len, device_version, NULL));
            device_version[version_len] = '\0';

            cl_uint compute_units;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_COMPUTE_UNITS, sizeof(compute_units), &compute_units, NULL));

            size_t ext_len;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_EXTENSIONS, 0, NULL, &ext_len));
            char *device_ext = (char *)malloc(ext_len + 1);
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_EXTENSIONS, ext_len, device_ext, NULL));
            device_ext[ext_len] = '\0';

            cl_ulong global_mem_size;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_GLOBAL_MEM_SIZE, sizeof(global_mem_size), &global_mem_size, NULL));

            cl_ulong max_alloc_size;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_MEM_ALLOC_SIZE, sizeof(max_alloc_size), &max_alloc_size, NULL));

            cl_ulong max_constant_size;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_CONSTANT_BUFFER_SIZE, sizeof(max_constant_size), &max_constant_size, NULL));

            cl_ulong local_mem_size;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_LOCAL_MEM_SIZE, sizeof(local_mem_size), &local_mem_size, NULL));

            cl_device_local_mem_type local_type;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_LOCAL_MEM_TYPE, sizeof(local_type), &local_type, NULL));

            size_t max_arg_size;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_PARAMETER_SIZE, sizeof(max_arg_size), &max_arg_size, NULL));

            printf("    %d compute units\n", compute_units);
            printf("    %lu bytes of global mem\n", global_mem_size);
            printf("    %lu byte max alloc size\n", max_alloc_size);
            printf("    %lu byte max constant size\n", max_constant_size);
            printf("    %lu byte local mem size (really local? %s)\n",
                    local_mem_size, local_type == CL_LOCAL ? "true" : "false");
            printf("    %lu byte max arg size\n", max_arg_size);
            printf("    %s\n", device_version);
            printf("    %s\n", device_ext);
#endif

            device_index++;
        }
        free(devices);
    }
    free(platforms);
}

void usage(char **argv) {
    fprintf(stderr, "usage: %s [-h] [-l]\n", argv[0]);
    fprintf(stderr, "    -h: Print this usage message\n");
    fprintf(stderr, "    -l: List device information on all devices\n");
    exit(1);
}

int main(int argc, char **argv) {
    int c;
    opterr = 0;
    if (argc == 1) {
        // No arguments
        usage(argv);
    }

    while ((c = getopt(argc, argv, "hl")) != -1) {
        switch (c) {
            case 'l':
                list_devices();
                return (0);
            case 'h':
                usage(argv);
        }
    }
}

