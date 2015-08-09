#ifndef BRIDGE_H
#define BRIDGE_H

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <map>
#include <set>
#include <string>
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

class mem_and_size {
    public:
        mem_and_size(cl_mem set_mem, size_t set_size) : mem(set_mem),
            size(set_size), valid(true) { }
        mem_and_size() : valid(false) { }

        cl_mem get_mem() { assert(valid); return mem; }
        size_t get_size() { assert(valid); return size; }
        bool is_valid() { return valid; }
    private:
        cl_mem mem;
        size_t size;
        bool valid;
};

typedef struct _device_context {
    cl_platform_id platform;
    cl_device_id dev;
    cl_context ctx;
    cl_command_queue cmd;

    pthread_mutex_t lock;

    map<jlong, cl_mem> *broadcast_cache;
    map<string, cl_program> *program_cache;
} device_context;

typedef struct _swat_context {
    // cl_program program;
    cl_kernel kernel;
    int host_thread_index;

    map<int, mem_and_size> *arguments;
    set<cl_mem> *all_allocated;
#ifdef BRIDGE_DEBUG
    map<int, kernel_arg *> *debug_arguments;
    char *kernel_src;
    size_t kernel_src_len;
#endif

} swat_context;

#endif
