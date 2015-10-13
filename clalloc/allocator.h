#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#ifndef OPENCL_ALLOCATOR
#ifndef CUDA_ALLOCATOR
// Default to OpenCL if neither is defined
#define OPENCL_ALLOCATOR
#endif
#endif

#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <stdio.h>

#ifdef OPENCL_ALLOCATOR
#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include "ocl_util.h"
#else

#include <cuda_runtime.h>

#define CHECK(call) { \
    cudaError_t __err = (call); \
    if (__err != cudaSuccess) { \
        fprintf(stderr, "CUDA Error at %s:%d - %s\n", \
                __FILE__, __LINE__, cudaGetErrorString(__err)); \
        exit(1); \
    } \
}

#define CHECK_ALLOC(ptr) { \
    if ((ptr) == NULL) { \
        fprintf(stderr, "Allocation failed at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
}

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
#ifdef OPENCL_ALLOCATOR
    cl_mem sub_mem;
#else
    char *sub_mem;
#endif
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
#ifdef OPENCL_ALLOCATOR
    cl_mem mem;
#else
    char *mem;
#endif
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
    int device_index;
} cl_allocator;

#ifdef OPENCL_ALLOCATOR
extern cl_allocator *init_allocator(cl_device_id dev, int device_index,
        cl_mem_flags alloc_flags, size_t limit_size, cl_context ctx,
        cl_command_queue cmd);
#else
extern cl_allocator *init_allocator(int device_index,
        double perc_high_performance_buffers);
#endif

extern bool re_allocate_cl_region(cl_region *target_region, int target_device);
extern cl_region *allocate_cl_region(size_t size, cl_allocator *allocator,
        void (*callback)(void *), void *user_data);
extern bool free_cl_region(cl_region *to_free, bool try_to_keep);
extern void print_allocator(cl_allocator *allocator, int lbl);
extern void bump_time(cl_allocator *allocator);
extern size_t count_free_bytes(cl_allocator *allocator);
extern unsigned long long get_contention(cl_allocator *allocator);

#define GET_DEVICE_FOR(my_region) ((my_region)->grandparent->allocator->device_index)

#endif
