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

#ifndef DEVICE_CONTEXT_H
#define DEVICE_CONTEXT_H

#include <pthread.h>
#include <map>
#include <string>
#include <jni.h>

#include "ocl_util.h"
#include "allocator.h"

#ifdef USE_CUDA
#include <nvrtc.h>
typedef CUmodule cl_program;
typedef CUfunction cl_kernel;
typedef CUevent cl_event;
#else
#include <CL/cl.h>
#endif

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
    unsigned id;

    void *pinned_h_heap;
    int *pinned_h_free_index;

    unsigned heap_size;

    struct _heap_context *next;

    // int h_heap_in_use;
    // pthread_mutex_t h_heap_lock;
    // pthread_cond_t h_heap_cond;
} heap_context;

typedef struct _device_context {
    cl_platform_id platform;
    cl_device_id dev;
#ifdef USE_CUDA
    CUcontext ctx;
    CUstream cmd;
#else
    cl_context ctx;
    cl_command_queue cmd;
#endif
    int device_index;
    int initialized;
    int count_heaps;

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

    cl_allocator *allocator;

    // Map from kernel name to PTX or to OpenCL program object
    map<string, cl_program> *program_cache;

    map<broadcast_id, cl_region *> *broadcast_cache;

    // List of free heaps
    heap_context *heap_cache_head;
    heap_context *heap_cache_tail;
    pthread_mutex_t heap_cache_lock;
    size_t heap_size;
    int n_heaps;
#ifdef PROFILE_LOCKS
    unsigned long long heap_cache_lock_contention;
    unsigned long long heap_cache_blocked;
#endif
} device_context;

#endif
