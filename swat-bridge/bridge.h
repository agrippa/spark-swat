#ifndef BRIDGE_H
#define BRIDGE_H

#include <stdio.h>
#include <map>

using namespace std;

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

#define JNI_JAVA(type, className, methodName) JNIEXPORT type JNICALL Java_org_apache_spark_rdd_cl_##className##_##methodName

typedef struct _swat_context {
    cl_platform_id platform;
    cl_device_id device;
    cl_context ctx;
    cl_program program;
    cl_kernel kernel;
    cl_command_queue cmd;

    map<int, cl_mem> *arguments;
} swat_context;

#endif
