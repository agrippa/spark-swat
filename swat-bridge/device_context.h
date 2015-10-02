#ifndef DEVICE_CONTEXT_H
#define DEVICE_CONTEXT_H

#ifdef __APPLE__
#include <cl.h>
#else
#include <CL/cl.h>
#endif

#include <pthread.h>
#include <map>
#include <string>
#include <jni.h>

#include "allocator.h"

using namespace std;

class broadcast_id {
    public:
        broadcast_id(int set_broadcast, int set_component) :
            broadcast(set_broadcast), component(set_component) { }

        bool operator<(const broadcast_id& other) const {
            if (broadcast < other.broadcast) {
                return true;
            } else if (broadcast > other.broadcast) {
                return false;
            }

            return component < other.component;
        }
    private:
        /*
         * Unique broadcast ID for this variable
         */
        int broadcast;
        /*
         * The component of this buffer we are storing (e.g. multiple buffers
         * are necessary to represent Tuple2 RDDs
         */
        int component;
};

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
    pthread_mutex_t broadcast_lock;
    pthread_mutex_t program_cache_lock;

    cl_allocator *allocator;

    map<string, cl_program> *program_cache;
    map<broadcast_id, cl_region *> *broadcast_cache;
} device_context;

#endif
