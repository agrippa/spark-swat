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

#ifndef BRIDGE_H
#define BRIDGE_H

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <map>
#include <vector>
#include <set>
#include <string>
#include <pthread.h>

#include "device_context.h"
#include "common.h"
#include "kernel_arg.h"
#include "allocator.h"

using namespace std;

#ifdef USE_CUDA
#include <cuda.h>

#define CHECK_NVRTC(call) { \
    const nvrtcResult _err = (call); \
    if (_err != NVRTC_SUCCESS) { \
        fprintf(stderr, "NVRTC Error @ %s:%d - %d\n", __FILE__, __LINE__, _err); \
        exit(1); \
    } \
}
#else
#include <CL/cl.h>
#endif

#define JNI_JAVA(type, className, methodName) JNIEXPORT type JNICALL Java_org_apache_spark_rdd_cl_##className##_##methodName

class mem_and_size {
    public:
        mem_and_size(cl_region *set_mem, size_t set_size) : mem(set_mem),
            size(set_size), valid(true) { }
        mem_and_size() : valid(false) { }

        cl_region *get_mem() { assert(valid); return mem; }
        size_t get_size() { assert(valid); return size; }
        bool is_valid() { return valid; }
    private:
        cl_region *mem;
        size_t size;
        bool valid;
};

class rdd_partition_offset {
    public:
        rdd_partition_offset(int set_rdd, int set_index, int set_offset, int set_component) :
            rdd(set_rdd), index(set_index), offset(set_offset), component(set_component) { }

        bool operator<(const rdd_partition_offset& other) const {
            if (rdd < other.rdd) {
                return true;
            } else if (rdd > other.rdd) {
                return false;
            }

            if (index < other.index) {
                return true;
            } else if (index > other.index) {
                return false;
            }

            if (offset < other.offset) {
                return true;
            } else if (offset > other.offset) {
                return false;
            }

            return component < other.component;
        }

        int get_partition() { return index; }

    private:
        // The RDD this buffer is a member of
        int rdd;
        // The partition in rdd
        int index;
        // The offset in elements inside the partition
        int offset;
        /*
         * The component of this buffer we are storing (e.g. multiple buffers
         * are necessary to represent Tuple2 RDDs
         */
        int component;
};

enum arg_type {
    REGION,
    INT,
    FLOAT,
    DOUBLE
};

typedef union _region_or_scalar {
    cl_region *region;
    int i;
    float f;
    double d;
} region_or_scalar;

typedef struct _arg_value {
    int index;
    bool keep; // only set for region type
    bool dont_free; // only set for region type
    bool copy_out; // only set for region type
    size_t len; // only set for region type
    enum arg_type type;
    region_or_scalar val;
} arg_value;

typedef struct _native_input_buffer_list_node {
    int id;
#ifdef USE_CUDA
    CUevent event;
#else
    cl_event event;
#endif
    struct _native_input_buffer_list_node *next;
} native_input_buffer_list_node;

typedef struct _event_info {
    unsigned long long timestamp;
#ifdef USE_CUDA
    CUevent event;
#else
    cl_event event;
#endif
    char *label;
    size_t metadata;
} event_info;

typedef struct _kernel_context kernel_context;
typedef struct _native_output_buffers native_output_buffers;

typedef struct _swat_context {
#ifdef USE_CUDA
    CUfunction kernel;

    void **kernel_buffers;
    unsigned kernel_buffers_capacity;
#else
    cl_kernel kernel;
#endif

    pthread_mutex_t kernel_lock;
#ifdef PROFILE_LOCKS
    unsigned long long kernel_lock_contention;
#endif

    int host_thread_index;

    arg_value *accumulated_arguments;
    int accumulated_arguments_len;
    int accumulated_arguments_capacity;

    arg_value *global_arguments;
    int global_arguments_len;
    int global_arguments_capacity;

    void *zeros;
    size_t zeros_capacity;

#ifdef USE_CUDA
    CUevent last_write_event;
    CUevent last_kernel_event;
#else
    cl_event last_write_event;
    cl_event last_kernel_event;
#endif

#ifdef PROFILE_OPENCL
    event_info *acc_write_events;
    int acc_write_events_capacity;
    int acc_write_events_length;
#endif

    native_input_buffer_list_node *freed_native_input_buffers;
    pthread_mutex_t freed_native_input_buffers_lock;
#ifdef PROFILE_LOCKS
    unsigned long long freed_native_input_buffers_lock_contention;
    unsigned long long freed_native_input_buffers_blocked;
#endif
    pthread_cond_t freed_native_input_buffers_cond;

    unsigned run_seq_no;

    kernel_context *completed_kernels;
    pthread_mutex_t completed_kernels_lock;
#ifdef PROFILE_LOCKS
    unsigned long long completed_kernels_lock_contention;
    unsigned long long completed_kernels_blocked;
#endif
    pthread_cond_t completed_kernels_cond;

#ifdef BRIDGE_DEBUG
    map<int, kernel_arg *> *debug_arguments;
    char *kernel_src;
    size_t kernel_src_len;
    int dump_index;
#endif

} swat_context;

typedef struct _heap_to_copy_back {
    kernel_context *kernel_ctx;
    unsigned heap_index;
    int kernel_complete;
} heap_to_copy_back;

/*
 * The host-side storage of a single heap instance transferred from the device.
 */
typedef struct _saved_heap {
    heap_context *heap_ctx;
    // void *h_heap;
    size_t size;
} saved_heap;

typedef struct _kernel_complete_flag {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int done;
    int host_thread_index; // for debugging
    int seq; // for debugging
} kernel_complete_flag;

struct _kernel_context {
    swat_context *ctx;
    device_context *dev_ctx;

    heap_context *curr_heap_ctx;
    saved_heap *heaps;
#ifdef USE_CUDA
    CUevent *heap_copy_back_events;
#else
    cl_event *heap_copy_back_events;
#endif
    int n_heap_ctxs;
    int heapStartArgnum;

    size_t n_loaded;
    size_t local_size;
    size_t global_size;

    unsigned seq_no;

    unsigned iter;
    int iterArgNum;

    kernel_context *next;

    // The set of arguments that are specific to this kernel instance
    arg_value *accumulated_arguments;
    int accumulated_arguments_len;

    int output_buffer_id;

    kernel_complete_flag *done_flag;

#ifdef PROFILE_OPENCL
    event_info *acc_write_events;
    int acc_write_events_length;
    int acc_write_events_capacity;
#endif
};


#endif
