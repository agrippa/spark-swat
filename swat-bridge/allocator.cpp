#include "allocator.h"

#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

extern void enter_trace(const char *lbl);
extern void exit_trace(const char *lbl);

#ifdef __cplusplus
}
#endif

static void lock_allocator(cl_allocator *allocator) {
    int perr = pthread_mutex_lock(&allocator->lock);
    assert(perr == 0);
}

static void unlock_allocator(cl_allocator *allocator) {
    int perr = pthread_mutex_unlock(&allocator->lock);
    assert(perr == 0);
}

static void bucket_insert_before(cl_region *target, cl_region *to_insert,
        cl_bucket *bucket) {
    enter_trace("bucket_insert_before");
    assert(target && to_insert);

    cl_region *prev = target->bucket_prev;
    target->bucket_prev = to_insert;
    if (prev) {
        prev->bucket_next = to_insert;
    } else {
        bucket->head = to_insert;
    }
    to_insert->bucket_next = target;
    to_insert->bucket_prev = prev;
    exit_trace("bucket_insert_before");
}

static void remove_from_bucket(cl_region *region) {
    enter_trace("remove_from_bucket");
    cl_bucket *bucket = region->parent;

    if (bucket->head == region && bucket->tail == region) {
        bucket->head = bucket->tail = NULL;
    } else if (bucket->head == region) {
        cl_region *new_head = bucket->head->bucket_next;
        new_head->bucket_prev = NULL;
        bucket->head = new_head;
    } else if (bucket->tail == region) {
        cl_region *new_tail = bucket->tail->bucket_prev;
        new_tail->bucket_next = NULL;
        bucket->tail = new_tail;
    } else {
        cl_region *prev = region->bucket_prev;
        cl_region *next = region->bucket_next;
        next->bucket_prev = prev;
        prev->bucket_next = next;
    }
    region->bucket_next = region->bucket_prev = NULL;
    region->parent = NULL;
    exit_trace("remove_from_bucket");
}

static void add_to_bucket(cl_region *region, cl_bucket *bucket) {
    enter_trace("add_to_bucket");
    assert(region->parent == NULL);

    if (bucket->head == NULL) {
        // empty bucket
        assert(bucket->tail == NULL);
        bucket->head = bucket->tail = region;
        region->bucket_next = NULL;
        region->bucket_prev = NULL;
    } else {
        cl_region *iter = bucket->head;
        bool inserted = false;
        while (iter) {
            if (iter->size >= region->size) {
                bucket_insert_before(iter, region, bucket);
                inserted = true;
                break;
            }
            iter = iter->bucket_next;
        }
        if (!inserted) {
            bucket->tail->bucket_next = region;
            region->bucket_prev = bucket->tail;
            region->bucket_next = NULL;
            bucket->tail = region;
        }
    }
    region->parent = bucket;
    exit_trace("add_to_bucket");
}

static void insert_into_buckets_helper(cl_region *region, cl_bucket *buckets,
        cl_bucket *large_bucket) {
    enter_trace("insert_into_buckets_helper");
    assert(region->size >= MIN_ALLOC_SIZE);
    bool inserted = false;
    for (int b = 0; b < NBUCKETS; b++) {
        if (BELONGS_TO_BUCKET(region->size, b)) {
            add_to_bucket(region, buckets + b);
            inserted = true;
            break;
        }
    }
    if (!inserted) {
        add_to_bucket(region, large_bucket);
    }
    exit_trace("insert_into_buckets_helper");
}

static void insert_into_buckets(cl_region *region, cl_alloc *alloc, bool try_to_keep) {
    enter_trace("insert_into_buckets");
    if (try_to_keep) {
        insert_into_buckets_helper(region, alloc->keep_buckets, &alloc->keep_large_bucket);
    } else {
        insert_into_buckets_helper(region, alloc->buckets, &alloc->large_bucket);
    }
    exit_trace("insert_into_buckets");
}

static cl_region *search_bucket(size_t rounded_size, cl_bucket *bucket) {
    enter_trace("search_bucket");
    cl_region *result = NULL;
    for (cl_region *r = bucket->head; r != NULL; r = r->bucket_next) {
        if (r->size >= rounded_size) {
            result = r;
            break;
        }
    }
    exit_trace("search_bucket");
    return result;
}

static void split(cl_region *target, size_t first_partition_size,
        cl_region **first_out, cl_region **second_out) {
    enter_trace("split");
    cl_alloc *alloc = target->grandparent;

    cl_region *new_region = (cl_region *)malloc(sizeof(cl_region));
    new_region->sub_mem = NULL;
    new_region->offset = target->offset + first_partition_size;
    new_region->size = target->size - first_partition_size;
    new_region->seq = target->seq + 1;
    new_region->parent = NULL;
    new_region->grandparent = target->grandparent;
    new_region->bucket_next = new_region->bucket_prev = NULL;
    new_region->next = target->next;
    new_region->prev = target;
    new_region->refs = 0;
    new_region->valid = false;
    new_region->free = true;

    remove_from_bucket(target);
    target->size = first_partition_size;
    target->next = new_region;
    target->seq = target->seq + 1;

    insert_into_buckets(target, alloc, false);
    insert_into_buckets(new_region, alloc, false);

    *first_out = target;
    *second_out = new_region;
    exit_trace("split");
}

static cl_region *split_from_front(cl_region *target, size_t rounded_size) {
    if (target->size == rounded_size) return target;
    cl_region *front, *back;
    split(target, rounded_size, &front, &back);
    return front;
}

static cl_region *split_from_back(cl_region *target, size_t rounded_size) {
    if (target->size == rounded_size) return target;
    cl_region *front, *back;
    split(target, target->size - rounded_size, &front, &back);
    return back;
}

static cl_region *find_matching_region_in_alloc(size_t rounded_size, cl_bucket *buckets) {
    cl_region *target_region = NULL;

    for (int b = 0; b < NBUCKETS && target_region == NULL; b++) {
        if (BUCKET_MIN_SIZE_INCL(b) <= rounded_size) {
            /*
             * Buckets are sorted smallest to larges. Allocations in each
             * bucket are also sorted smallest to largest. This means that
             * the first allocation we find with sufficient size is also the
             * tightest fit possible.
             */
            cl_bucket *bucket = buckets + b;
            cl_region *match = search_bucket(rounded_size, bucket);
            if (match) {
                target_region = match;
            }
        }
    }

    return target_region;
}

void add_hold(cl_region *target) {
    enter_trace("add_hold");

    lock_allocator(target->grandparent->allocator);

    target->refs += 1;

    unlock_allocator(target->grandparent->allocator);

    exit_trace("add_hold");
}

bool free_cl_region(cl_region *to_free, bool try_to_keep) {
    enter_trace("free_cl_region");

    lock_allocator(to_free->grandparent->allocator);

    assert(!to_free->free);
    assert(to_free->valid);
    to_free->refs = to_free->refs - 1;

    if (to_free->refs == 0) {
        insert_into_buckets(to_free, to_free->grandparent, try_to_keep);
        to_free->free = true;
    }

    bool return_value = (to_free->refs == 0);

    unlock_allocator(to_free->grandparent->allocator);

    exit_trace("free_cl_region");
    return return_value;
}

void re_allocate_cl_region(cl_region *target_region, size_t expected_seq) {
    enter_trace("re_allocate_cl_region");

    lock_allocator(target_region->grandparent->allocator);

    remove_from_bucket(target_region);
    assert(expected_seq == target_region->seq);
    assert(target_region->valid && target_region->free &&
            target_region->refs == 0);
    target_region->free = false;
    target_region->refs = 1;

    unlock_allocator(target_region->grandparent->allocator);

    exit_trace("re_allocate_cl_region");
}

cl_region *allocate_cl_region(size_t size, cl_allocator *allocator) {
    enter_trace("allocate_cl_region");
    assert(allocator);

    lock_allocator(allocator);

    size_t rounded_size = pow(2, ceil(log(size)/log(2)));

    cl_region *target_region = NULL;

    // First look in the normal buckets
    for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
        cl_alloc *alloc = allocator->allocs + a;
        target_region = find_matching_region_in_alloc(rounded_size, alloc->buckets);
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching buckets\n", target_region);
#endif

    // Then look in the large normal buckets
    if (target_region == NULL) {
        for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            target_region = search_bucket(rounded_size, &alloc->large_bucket);
        }
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching large buckets\n", target_region);
#endif

    // Then look in the keep buckets
    if (target_region == NULL) {
        for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            target_region = find_matching_region_in_alloc(rounded_size, alloc->keep_buckets);
        }
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching keep_buckets\n", target_region);
#endif

    // Then look in the large keep buckets
    if (target_region == NULL) {
        for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            target_region = search_bucket(rounded_size, &alloc->keep_large_bucket);
        }
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching keep_large_buckets\n", target_region);
#endif

    if (target_region && target_region->size > rounded_size) {
        // Split region
        cl_region *prev_region = target_region->prev;
        cl_region *next_region = target_region->next;

        if (next_region && prev_region && next_region->size > prev_region->size) {
            /*
             * Only prefer to split from the end if the next region is larger
             * than the previous.
             */
            target_region = split_from_back(target_region, rounded_size);
        } else {
            target_region = split_from_front(target_region, rounded_size);
        }
    }

    if (target_region) {
        /*
         * Now we have a target_region that exactly matches the desired size of the
         * allocation (rounded), but is still in the buckets and region list.
         */
        cl_alloc *alloc = target_region->grandparent;
        remove_from_bucket(target_region);
        assert(target_region->free && target_region->refs == 0);
        target_region->free = false;
        target_region->refs = 1;
        target_region->seq = target_region->seq + 1;
        if (!target_region->valid) {
            cl_int err;
            cl_buffer_region sub_region;
            sub_region.origin = target_region->offset;
            sub_region.size = target_region->size;
            target_region->sub_mem = clCreateSubBuffer(alloc->mem, 0,
                    CL_BUFFER_CREATE_TYPE_REGION, &sub_region, &err);
            CHECK(err);
            target_region->valid = true;
        }
    }

    exit_trace("allocate_cl_region");

    unlock_allocator(allocator);

    return target_region;
}

cl_allocator *init_allocator(cl_device_id dev, cl_context ctx) {
    enter_trace("init_allocator");
    cl_ulong global_mem_size, max_alloc_size;
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_GLOBAL_MEM_SIZE,
                sizeof(global_mem_size), &global_mem_size, NULL));
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_MEM_ALLOC_SIZE,
                sizeof(max_alloc_size), &max_alloc_size, NULL));

    int nallocs = (global_mem_size + max_alloc_size - 1) / max_alloc_size;

    cl_allocator *allocator = (cl_allocator *)malloc(sizeof(cl_allocator));
    allocator->nallocs = nallocs;
    allocator->allocs = (cl_alloc *)malloc(nallocs * sizeof(cl_alloc));

    int perr = pthread_mutex_init(&allocator->lock, NULL);
    assert(perr == 0);

    for (int i = 0; i < nallocs; i++) {
        cl_ulong alloc_size = max_alloc_size;
        cl_ulong leftover = global_mem_size - (i * max_alloc_size);
        if (leftover < alloc_size) {
            alloc_size = leftover;
        }

        cl_int err;
        cl_mem mem = clCreateBuffer(ctx, CL_MEM_READ_WRITE, alloc_size, NULL, &err);
        CHECK(err);
        (allocator->allocs)[i].mem = mem;
        (allocator->allocs)[i].size = alloc_size;
        (allocator->allocs)[i].allocator = allocator;

        cl_region *first_region = (cl_region *)malloc(sizeof(cl_region));
        first_region->sub_mem = NULL;
        first_region->offset = 0;
        first_region->size = alloc_size;
        first_region->seq = 0;
        first_region->parent = NULL;
        first_region->grandparent = allocator->allocs + i;
        first_region->bucket_next = NULL;
        first_region->bucket_prev = NULL;
        first_region->next = NULL;
        first_region->prev = NULL;
        first_region->refs = 0;
        first_region->valid = false;
        first_region->free = true;

        for (int b = 0; b < NBUCKETS; b++) {
            (allocator->allocs)[i].buckets[b].parent = allocator->allocs + i;
            (allocator->allocs)[i].buckets[b].head = NULL;
            (allocator->allocs)[i].buckets[b].tail = NULL;
            (allocator->allocs)[i].keep_buckets[b].head = NULL;
            (allocator->allocs)[i].keep_buckets[b].tail = NULL;
        }
        (allocator->allocs)[i].large_bucket.parent = allocator->allocs + i;
        (allocator->allocs)[i].keep_large_bucket.parent = allocator->allocs + i;
        (allocator->allocs)[i].large_bucket.head = NULL;
        (allocator->allocs)[i].large_bucket.tail = NULL;
        (allocator->allocs)[i].keep_large_bucket.head = NULL;
        (allocator->allocs)[i].keep_large_bucket.tail = NULL;

        (allocator->allocs)[i].region_list = first_region;
        insert_into_buckets(first_region, allocator->allocs + i, false);
    }
    exit_trace("init_allocator");

    return allocator;
}
