#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include <CL/cl.h>
#include <assert.h>
#include "ocl_util.h"

#define MIN_ALLOC_SIZE  (1 << 10)
#define NBUCKETS    10

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

typedef struct _cl_region {
    cl_mem sub_mem;
    size_t offset, size;
    size_t seq;
    cl_bucket *parent;
    cl_alloc *grandparent;
    cl_region *bucket_next, *bucket_prev;
    cl_region *next, *prev;
    int refs;
    bool valid;
    bool free;
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
    cl_region *region_list;
} cl_alloc;

typedef struct _cl_allocator {
    cl_alloc *allocs;
    int nallocs;
} cl_allocator;

extern cl_allocator *init_allocator(cl_device_id dev, cl_context ctx);
extern cl_region *allocate_cl_region(size_t size, cl_allocator *allocator);
extern bool free_cl_region(cl_region *to_free, bool try_to_keep);
extern void add_hold(cl_region *target);

#endif
