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
         * are necessary to represent Tuple2 RDDs, dense vectors, sparse
         * vectors, etc)
         */
        int component;
};

typedef struct _device_context device_context;

typedef struct _heap_context {
    device_context *dev_ctx;
    cl_region *heap;
    cl_region *free_index;

    void *pinned_h_heap;
    int *pinned_h_free_index;

    unsigned heap_size;

    struct _heap_context *next;

    int h_heap_in_use;
    pthread_mutex_t h_heap_lock;
    pthread_cond_t h_heap_cond;
} heap_context;

typedef struct _device_context {
    cl_platform_id platform;
    cl_device_id dev;
    cl_context ctx;
    cl_command_queue cmd;
    int device_index;
    int initialized;

    /*
     * Locked for setting args on device (for broadcast cache?) and when
     * building a program for a new device (which should happen infrequently).
     */
    pthread_mutex_t broadcast_lock;
#ifdef PROFILE_LOCKS
    unsigned long long broadcast_lock_contention;
#endif

    pthread_mutex_t program_cache_lock;
#ifdef PROFILE_LOCKS
    unsigned long long program_cache_lock_contention;
#endif

    cl_allocator *heap_allocator;
    cl_allocator *allocator;

    map<string, cl_program> *program_cache;
    map<broadcast_id, cl_region *> *broadcast_cache;

    heap_context *heap_cache_head;
    heap_context *heap_cache_tail;
    pthread_mutex_t heap_cache_lock;
    pthread_cond_t heap_cache_cond;
    int n_heaps;
#ifdef PROFILE_LOCKS
    unsigned long long heap_cache_lock_contention;
    unsigned long long heap_cache_blocked;
#endif
} device_context;

#endif
