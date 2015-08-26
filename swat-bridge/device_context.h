#ifndef DEVICE_CONTEXT_H
#define DEVICE_CONTEXT_H

#include <CL/cl.h>
#include <pthread.h>
#include <map>
#include <string>
#include <jni.h>

#include "allocator.h"

using namespace std;

typedef struct _device_context {
    cl_platform_id platform;
    cl_device_id dev;
    cl_context ctx;
    cl_command_queue cmd;
    int device_index;

    /*
     * Locked for setting args on device (for broadcast cache?) and when
     * building a program for a new device (which should happen infrequently).
     */
    pthread_rwlock_t broadcast_lock;
    pthread_mutex_t program_cache_lock;

    cl_allocator *allocator;

    map<string, cl_program> *program_cache;
    map<jlong, cl_region *> *broadcast_cache;
    // map<rdd_partition_offset, cl_region *> *rdd_cache;
} device_context;

#endif
