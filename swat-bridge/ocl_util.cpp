#include "ocl_util.h"

cl_uint get_num_opencl_platforms() {
    cl_uint num_platforms;
    CHECK(clGetPlatformIDs(0, NULL, &num_platforms));
    return num_platforms;
}

cl_uint get_num_devices(cl_platform_id platform, cl_device_type type) {
    cl_uint num_devices;
    cl_int err = clGetDeviceIDs(platform, type, 0, NULL, &num_devices);
    if (err == CL_DEVICE_NOT_FOUND) {
        return 0;
    } else {
        CHECK(err);
    }
    return num_devices;
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
