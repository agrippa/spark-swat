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

cl_uint get_total_num_devices() {
    cl_uint count_devices = 0;
    cl_uint num_platforms = get_num_opencl_platforms();

    cl_platform_id *platforms =
        (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
    CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));

    for (cl_uint platform_index = 0; platform_index < num_platforms;
            platform_index++) {
        count_devices += get_num_devices(platforms[platform_index]);
    }

    free(platforms);

    return count_devices;
}

char *get_device_name(cl_device_id device) {
    size_t name_len;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_NAME, 0, NULL, &name_len));
    char *device_name = (char *)malloc(name_len + 1);
    CHECK(clGetDeviceInfo(device, CL_DEVICE_NAME, name_len, device_name,
                NULL));
    device_name[name_len] = '\0';
    return device_name;
}

const char *get_device_type_str(cl_device_id device) {
    cl_device_type type = get_device_type(device);
    switch (type) {
        case (CL_DEVICE_TYPE_GPU):
            return "GPU";
        case (CL_DEVICE_TYPE_CPU):
            return "CPU";
        default:
            fprintf(stderr, "Unsupported device type %d\n", type);
            exit(1);
    }
}

cl_device_type get_device_type(cl_device_id device) {
    cl_device_type type;
    CHECK(clGetDeviceInfo(device, CL_DEVICE_TYPE, sizeof(type), &type, NULL));
    return type;
}
