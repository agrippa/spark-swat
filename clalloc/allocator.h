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

#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <stdio.h>

#include "ocl_util.h"

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
typedef CUdeviceptr cl_mem;
#else
#include <CL/cl.h>
#endif

#define ASSERT_MSG(conditional, msg) { \
    if (!(conditional)) { \
        fprintf(stderr, "Assertion failure at %s:%d - %s\n", __FILE__, \
                __LINE__, msg); \
        exit(1); \
    } \
}

#define ASSERT(conditional) { \
    if (!(conditional)) { \
        fprintf(stderr, "Assertion failure at %s:%d\n", __FILE__, \
                __LINE__); \
        exit(1); \
    } \
}


// #define MIN_ALLOC_SIZE  (1 << 10)
#define MIN_ALLOC_SIZE  1
#define NBUCKETS    20


#ifdef TRACE
#ifdef __cplusplus
extern "C" {
#endif

extern void enter_trace(const char *lbl);
extern void exit_trace(const char *lbl);

#ifdef __cplusplus
}
#endif

#define ENTER_TRACE(lbl) enter_trace(lbl)
#define EXIT_TRACE(lbl) exit_trace(lbl)
#else
#define ENTER_TRACE(lbl)
#define EXIT_TRACE(lbl)
#endif

#define BUCKET_MIN_SIZE_INCL(my_bucket) ((size_t)(MIN_ALLOC_SIZE * (2 << (my_bucket))))
#define BUCKET_MAX_SIZE_EXCL(my_bucket) ((size_t)(BUCKET_MIN_SIZE_INCL(my_bucket + 1)))
#define BELONGS_TO_BUCKET(my_size, my_bucket) \
    (my_size >= BUCKET_MIN_SIZE_INCL(my_bucket) && \
     my_size < BUCKET_MAX_SIZE_EXCL(my_bucket))

struct _cl_bucket;
typedef struct _cl_bucket cl_bucket;
struct _cl_region;
typedef struct _cl_region cl_region;
struct _cl_alloc;
typedef struct _cl_alloc cl_alloc;
struct _cl_allocator;
typedef struct _cl_allocator cl_allocator;

typedef struct _cl_region {
    cl_mem sub_mem;
    size_t offset, size;
    cl_bucket *parent;
    cl_alloc *grandparent;
    cl_region *bucket_next, *bucket_prev;
    cl_region *next, *prev;
    int refs;
    bool keeping;
    long birth;
    /*
     * assume there can only be one outstanding reference to a region in one
     * cache
     */
    bool invalidated;
} cl_region;

typedef struct _cl_bucket {
    cl_alloc *parent;
    cl_region *head, *tail;
} cl_bucket;

typedef struct _cl_alloc {
    cl_mem mem;
    char *pinned;
    size_t size;
    size_t free_bytes; // purely for diagnostics and error-checking
    cl_bucket buckets[NBUCKETS];
    cl_bucket large_bucket;
    cl_bucket keep_buckets[NBUCKETS];
    cl_bucket keep_large_bucket;
    cl_region *region_list_head;

    cl_allocator *allocator;
    long curr_time;
    pthread_mutex_t lock;
#ifdef PROFILE_LOCKS
    unsigned long long contention;
#endif
} cl_alloc;

/*
 * There is a one-to-one mapping between allocators and OpenCL devices. All of
 * the fields of the allocator object are constant after creation. The allocator
 * is the root of a region of cl_alloc objects, each representing a subset of
 * the memory in a device.
 */
typedef struct _cl_allocator {
    cl_alloc *allocs;
    int nallocs;
    unsigned int address_align;
#ifdef USE_CUDA
    CUcontext cu_ctx;
#endif
    int device_index;
} cl_allocator;

#ifdef USE_CUDA
/*
 * Assumes that the calling thread has already attached a CUDA context. This
 * call asserts that the current context matches the expected device index.
 */
extern cl_allocator *init_allocator(CUcontext ctx);
#else
extern cl_allocator *init_allocator(cl_device_id dev, int device_index,
        cl_mem_flags alloc_flags, size_t limit_size, cl_context ctx,
        cl_command_queue cmd);
#endif

extern bool re_allocate_cl_region(cl_region *target_region, int target_device);
extern cl_region *allocate_cl_region(size_t size, cl_allocator *allocator,
        void (*callback)(void *), void *user_data);
extern bool free_cl_region(cl_region *to_free, bool try_to_keep);
extern void print_allocator(cl_allocator *allocator, int lbl);
extern void bump_time(cl_allocator *allocator);
extern size_t count_free_bytes(cl_allocator *allocator);
extern unsigned long long get_contention(cl_allocator *allocator);
extern void print_clalloc_profile(int thread);
extern void *fetch_pinned(cl_region *region);

#define GET_DEVICE_FOR(my_region) ((my_region)->grandparent->allocator->device_index)

#endif
