#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include <CL/cl.h>

#include "ocl_util.h"

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
    size_t size;
    cl_bucket buckets[NBUCKETS];
    cl_bucket large_bucket;
    cl_bucket keep_buckets[NBUCKETS];
    cl_bucket keep_large_bucket;
    cl_region *region_list_head;

    cl_allocator *allocator;
} cl_alloc;

typedef struct _cl_allocator {
    cl_alloc *allocs;
    int nallocs;
    long curr_time;
    cl_uint address_align;
    int device_index;

    pthread_mutex_t lock;
} cl_allocator;

extern bool re_allocate_cl_region(cl_region *target_region, int target_device);
extern cl_allocator *init_allocator(cl_device_id dev, int device_index,
        cl_context ctx, cl_command_queue cmd);
extern cl_region *allocate_cl_region(size_t size, cl_allocator *allocator);
extern bool free_cl_region(cl_region *to_free, bool try_to_keep);
extern void print_allocator(cl_allocator *allocator, int lbl);
extern void bump_time(cl_allocator *allocator);

#define GET_DEVICE_FOR(my_region) ((my_region)->grandparent->allocator->device_index)

#endif
