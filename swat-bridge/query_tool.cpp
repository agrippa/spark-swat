#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <map>

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include "ocl_util.h"

void list_devices() {
    cl_uint num_platforms = get_num_opencl_platforms();
    cl_platform_id *platforms =
        (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
    CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));
    int device_index = 0;

    for (cl_uint platform_index = 0; platform_index < num_platforms;
            platform_index++) {
        cl_uint num_devices = get_num_devices(platforms[platform_index]);

        printf("Platform %d (%d devices)\n", platform_index, num_devices);

        cl_device_id *devices = (cl_device_id *)malloc(sizeof(cl_device_id) *
                num_devices);
        CHECK(clGetDeviceIDs(platforms[platform_index], CL_DEVICE_TYPE_ALL,
                    num_devices, devices, NULL));
        for (unsigned d = 0; d < num_devices; d++) {
            cl_device_id dev = devices[d];
            cl_device_type type;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_TYPE, sizeof(type), &type,
                        NULL));
            printf("  Device %d - ", device_index);

            if (type == CL_DEVICE_TYPE_GPU) {
                printf("GPU");
            } else if (type == CL_DEVICE_TYPE_CPU) {
                printf("CPU");
            } else {
                fprintf(stderr, "Unsupported device type in list_devices\n");
                exit(1);
            }

            printf(" - ");

            size_t name_len;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_NAME, 0, NULL, &name_len));
            char *device_name = (char *)malloc(name_len + 1);
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_NAME, name_len, device_name,
                        NULL));
            device_name[name_len] = '\0';
            printf("%s\n", device_name);

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

