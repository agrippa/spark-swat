#ifndef BRIDGE_H
#define BRIDGE_H

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <map>
#include <pthread.h>

#include "common.h"
#include "kernel_arg.h"

using namespace std;

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#define JNI_JAVA(type, className, methodName) JNIEXPORT type JNICALL Java_org_apache_spark_rdd_cl_##className##_##methodName

typedef struct _device_context {
    cl_platform_id platform;
    cl_device_id dev;
    cl_context ctx;
    cl_command_queue cmd;

    pthread_mutex_t lock;

    map<jlong, cl_mem> *broadcast_cache;
} device_context;

typedef struct _swat_context {
    cl_program program;
    cl_kernel kernel;

    map<int, cl_mem> *arguments;
#ifdef BRIDGE_DEBUG
    map<int, kernel_arg *> *debug_arguments;
    char *kernel_src;
    size_t kernel_src_len;
#endif

} swat_context;

#endif
