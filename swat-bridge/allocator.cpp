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

static void bucket_insert_after(cl_region *target, cl_region *to_insert,
        cl_bucket *bucket) {
    enter_trace("bucket_insert_after");
    assert(target && to_insert);

    cl_region *next = target->bucket_next;
    target->bucket_next = to_insert;
    if (next) {
        next->bucket_prev = to_insert;
    } else {
        bucket->tail = to_insert;
    }
    to_insert->bucket_prev = target;
    to_insert->bucket_next = next;
    exit_trace("bucket_insert_after");
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

static void add_to_normal_bucket(cl_region *region, cl_bucket *bucket) {
    enter_trace("add_to_normal_bucket");
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
    exit_trace("add_to_normal_bucket");
}

static void add_to_keep_bucket(cl_region *region, cl_bucket *bucket) {
    enter_trace("add_to_keep_bucket");
    assert(region->parent == NULL);

    if (bucket->head == NULL) {
        // empty bucket
        assert(bucket->tail == NULL);
        bucket->head = bucket->tail = region;
        region->bucket_next = NULL;
        region->bucket_prev = NULL;
    } else {
        cl_region *iter = bucket->tail;
        bool inserted = false;
        while (iter) {
            if (iter->birth == region->birth) {
                if (iter->size < region->size) {
                    bucket_insert_after(iter, region, bucket);
                    inserted = true;
                    break;
                }
            } else if (iter->birth < region->birth) {
                bucket_insert_after(iter, region, bucket);
                inserted = true;
                break;
            }
            iter = iter->bucket_prev;
        }
        if (!inserted) {
            bucket->head->bucket_prev = region;
            region->bucket_next = bucket->head;
            region->bucket_prev = NULL;
            bucket->head = region;
        }
    }
    region->parent = bucket;
    exit_trace("add_to_keep_bucket");
}

static void insert_into_buckets_helper(cl_region *region, cl_bucket *buckets,
        cl_bucket *large_bucket) {
    enter_trace("insert_into_buckets_helper");
    assert(region->size >= MIN_ALLOC_SIZE);
    bool inserted = false;
    for (int b = 0; b < NBUCKETS; b++) {
        if (BELONGS_TO_BUCKET(region->size, b)) {
            if (region->keeping) {
                add_to_keep_bucket(region, buckets + b);
            } else {
                add_to_normal_bucket(region, buckets + b);
            }
            inserted = true;
            break;
        }
    }
    if (!inserted) {
        if (region->keeping) {
            add_to_keep_bucket(region, large_bucket);
        } else {
            add_to_normal_bucket(region, large_bucket);
        }
    }
    exit_trace("insert_into_buckets_helper");
}

static void insert_into_buckets(cl_region *region, cl_alloc *alloc, bool try_to_keep) {
    enter_trace("insert_into_buckets");
    region->keeping = try_to_keep;
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

/*
 * On a split there's never any reason to update alloc->region_list_head,
 * because the split always splits off a region from the back of target (and not
 * the front), this can't change the head of the list.
 */
static void split(cl_region *target, size_t first_partition_size,
        cl_region **first_out, cl_region **second_out) {
    enter_trace("split");
    cl_alloc *alloc = target->grandparent;

    cl_region *new_region = (cl_region *)malloc(sizeof(cl_region));

#ifdef VERBOSE
    fprintf(stderr, "Splitting (%p offset=%lu size=%lu) into %p and %p, "
            "prev=%p, next=%p\n", target, target->offset, target->size, target,
            new_region, target->prev, target->next);
#endif

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
    new_region->keeping = false;
    new_region->free = true;
    new_region->merged = false;
    new_region->cached = false;

    remove_from_bucket(target);
    target->size = first_partition_size;
    target->next = new_region;
    target->seq = target->seq + 1;
    target->keeping = false;

    if (new_region->next) {
        new_region->next->prev = new_region;
    }

#ifdef VERBOSE
    fprintf(stderr, "  target=(%p offset=%lu size=%lu) new_region=(%p "
            "offset=%lu size=%lu)\n", target, target->offset, target->size,
            new_region, new_region->offset, new_region->size);
#endif

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

/*
 * free_cl_region always merges later ranges into earlier ranges, so no need to
 * update region_list_head.
 */
bool free_cl_region(cl_region *to_free, bool try_to_keep) {
    enter_trace("free_cl_region");

    lock_allocator(to_free->grandparent->allocator);

    assert(!to_free->free);
    assert(to_free->valid);
    to_free->refs = to_free->refs - 1;

    if (to_free->refs == 0) {
        cl_region *next = to_free->next;
        cl_region *prev = to_free->prev;
#ifdef VERBOSE
        fprintf(stderr, "Before freeing (%p offset=%lu size=%lu)\n", to_free,
                to_free->offset, to_free->size);
        fprintf(stderr, "  try_to_keep=%d next=%p prev=%p\n", try_to_keep, next,
                prev);
        if (prev) {
            fprintf(stderr, "  prev=(offset=%lu size=%lu)\n", prev->offset, prev->size);
        }
        if (next) {
            fprintf(stderr, "  next=(offset=%lu size=%lu)\n", next->offset, next->size);
        }
        print_allocator(to_free->grandparent->allocator, -1);
#endif
        assert(next == NULL || next->prev == to_free);
        assert(prev == NULL || prev->next == to_free);
        assert(next == NULL || next->offset == to_free->offset + to_free->size);
        assert(prev == NULL || to_free->offset == prev->offset + prev->size);
        to_free->keeping = try_to_keep;
        if (!try_to_keep) to_free->birth = 0;
        to_free->cached = try_to_keep;

        if (!try_to_keep && (next && next->free && !next->keeping) &&
                (prev && prev->free && !prev->keeping)) {
            // Merge with next and prev
#ifdef VERBOSE
            fprintf(stderr, "Merging prev=(%p offset=%lu size=%lu) and "
                    "next=(%p offset=%lu size=%lu) for to_free=(%p offset=%lu "
                    "size=%lu)\n", prev, prev->offset, prev->size, next,
                    next->offset, next->size, to_free, to_free->offset,
                    to_free->size);
#endif
            prev->size += to_free->size + next->size;
            // seq doesn't matter because none are marked keeping
            // parent will be handled when we remove from bucket and re-insert
            // grandparent remains the same
            // bucket_next, bucket_prev will be handled on remove and re-insert
            prev->next = next->next;
            if (prev->next) {
                prev->next->prev = prev;
            }
#ifdef VERBOSE
            fprintf(stderr, "  prev=(%p offset=%lu size=%lu)\n", prev,
                    prev->offset, prev->size);
#endif
            // prev remains the same
            // refs remain the same
            prev->valid = false;
            // free remains the same
            // keeping remains the same
            // birth doesn't matter for !keeping
            remove_from_bucket(next);
            free(next);
            free(to_free);
            remove_from_bucket(prev);
            insert_into_buckets(prev, prev->grandparent, false);
        } else if (!try_to_keep && next && next->free && !next->keeping) {
            // Merge with just next
#ifdef VERBOSE
            fprintf(stderr, "Merging next=(%p offset=%lu size=%lu) for "
                    "to_free=(%p offset=%lu size=%lu)\n", next, next->offset,
                    next->size, to_free, to_free->offset, to_free->size);
#endif

            to_free->size += next->size;
            // seq doesn't matter because none are marked keeping
            // parent will be handled when we remove from bucket and re-insert
            // grandparent remains the same
            // bucket_next, bucket_prev will be handled on remove and re-insert
            to_free->next = next->next;
            if (to_free->next) {
                to_free->next->prev = to_free;
            }
#ifdef VERBOSE
            fprintf(stderr, "  to_free=(%p offset=%lu size=%lu)\n", to_free,
                    to_free->offset, to_free->size);
#endif
            // prev remains the same
            // refs is already 0
            to_free->valid = false;
            to_free->free = true;
            // keeping handled by insert_into_buckets
            // birth doesn't matter for !keeping
            remove_from_bucket(next);
            free(next);
            insert_into_buckets(to_free, to_free->grandparent, false);
        } else if (!try_to_keep && prev && prev->free && !prev->keeping) {
            // Merge with just prev
#ifdef VERBOSE
            fprintf(stderr, "Merging prev=(%p offset=%lu size=%lu) for "
                    "to_free=(%p offset=%lu size=%lu)\n", prev, prev->offset,
                    prev->size, to_free, to_free->offset, to_free->size);
#endif

            prev->size += to_free->size;
            prev->next = next;
            if (prev->next) {
                prev->next->prev = prev;
            }
#ifdef VERBOSE
            fprintf(stderr, "  prev=(%p offset=%lu size=%lu)\n", prev,
                    prev->offset, prev->size);
#endif

            prev->valid = false;
            free(to_free);
            remove_from_bucket(prev);
            insert_into_buckets(prev, prev->grandparent, false);
        } else {
            insert_into_buckets(to_free, to_free->grandparent, try_to_keep);
            to_free->free = true;
            to_free->keeping = try_to_keep;
        }
#ifdef VERBOSE
        fprintf(stderr, "After freeing (%p %lu %lu)\n", to_free,
                to_free->offset, to_free->size);
        print_allocator(to_free->grandparent->allocator, -1);
#endif

    }

    bool return_value = (to_free->refs == 0);

    unlock_allocator(to_free->grandparent->allocator);

    exit_trace("free_cl_region");
    return return_value;
}

bool re_allocate_cl_region(cl_region *target_region, size_t expected_seq) {
    enter_trace("re_allocate_cl_region");

    lock_allocator(target_region->grandparent->allocator);

    if (target_region->seq != expected_seq) {
        return false;
    }
    if (target_region->merged) {
        free(target_region);
        return false;
    }

    if (target_region->free) {
#ifdef VERBOSE
        fprintf(stderr, "Before Re-Allocating (%p %lu %lu)\n", target_region,
                target_region->offset, target_region->size);
        print_allocator(target_region->grandparent->allocator, -1);
#endif
        remove_from_bucket(target_region);
        assert(target_region->valid && target_region->free &&
                target_region->refs == 0);
        target_region->free = false;
        target_region->refs = 1;
#ifdef VERBOSE
        fprintf(stderr, "After Re-Allocating (%p %lu %lu)\n", target_region,
                target_region->offset, target_region->size);
        print_allocator(target_region->grandparent->allocator, -1);
#endif

    } else {
        target_region->refs += 1;
    }
    target_region->birth = target_region->grandparent->allocator->curr_time;
    target_region->merged = false;

    unlock_allocator(target_region->grandparent->allocator);

    exit_trace("re_allocate_cl_region");

    return true;
}

cl_region *allocate_cl_region(size_t size, cl_allocator *allocator) {
    enter_trace("allocate_cl_region");
    assert(allocator);

    lock_allocator(allocator);

    size_t rounded_size = pow(2, ceil(log(size)/log(2)));

#ifdef VERBOSE
    fprintf(stderr, "Allocatin %lu bytes:\n", rounded_size);
    print_allocator(allocator, -1);
#endif

    cl_region *target_region = NULL;

    // First look in the normal buckets
    for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
        cl_alloc *alloc = allocator->allocs + a;
        target_region = find_matching_region_in_alloc(rounded_size, alloc->buckets);
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching buckets for region of "
            "size %lu\n", target_region, rounded_size);
#endif

    // Then look in the large normal buckets
    if (target_region == NULL) {
        for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            target_region = search_bucket(rounded_size, &alloc->large_bucket);
        }
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching large buckets for "
            "region of size %lu\n", target_region, rounded_size);
#endif

    // Then look in the keep buckets
    if (target_region == NULL) {
        for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            target_region = find_matching_region_in_alloc(rounded_size, alloc->keep_buckets);
        }
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching keep_buckets for "
            "region of size %lu\n", target_region, rounded_size);
#endif

    // Then look in the large keep buckets
    if (target_region == NULL) {
        for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            target_region = search_bucket(rounded_size, &alloc->keep_large_bucket);
        }
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching keep_large_buckets "
            "for region of size %lu\n", target_region, rounded_size);
#endif

    if (target_region == NULL) {
        /*
         * All allocation attempts failed, seems like we need to merge some free
         * keep regions with some free non-keep regions (either that or memory
         * is so segmented that this allocation is doomed).
         */
        cl_region *best_candidate = NULL;
        int best_candidate_successors = -1;
        long best_candidate_birth = -1;

        for (int a = 0; a < allocator->nallocs; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            cl_region *curr = alloc->region_list_head;

            while (curr) {
                size_t acc_bytes = curr->size;
                long youngest = curr->birth;
                int count_successors = 0;
                cl_region *succ = curr->next;

                while (succ && succ->free && acc_bytes < rounded_size) {
                    acc_bytes += succ->size;
                    youngest = (succ->birth > youngest ? succ->birth : youngest);
                    count_successors++;
                    succ = succ->next;
                }

                /*
                 * Look for the group with the oldest, youngest member
                 */
                if (acc_bytes >= rounded_size && (best_candidate == NULL ||
                            youngest < best_candidate_birth)) {
                    best_candidate = curr;
                    best_candidate_successors = count_successors;
                    best_candidate_birth = youngest;
                }

                curr = curr->next;
            }
        }

        assert(best_candidate);
        cl_region *succ = best_candidate->next;
        int count_succ = 0;
        while (count_succ < best_candidate_successors) {

            best_candidate->size += succ->size;
            // no need to handle parent
            // grandparent unchanged
            // bucket_next, bucket_prev will be handled later
            best_candidate->next = succ->next;  // eventually the last successor->next
            // prev remains the same
            // refs is already 0

            count_succ++;
            cl_region *next = succ->next;

            remove_from_bucket(succ);
            /*
             * cached is set in free_cl_region when an item is finally freed, so
             * we know it must be set to the actual value here because we are
             * only dealing with freed objects.
             */
            if (succ->cached) {
                succ->next = NULL;
                succ->prev = NULL;
                succ->merged = true;
            } else {
                free(succ);
            }
            succ = next;
        }

        if (best_candidate->next) {
            best_candidate->next->prev = best_candidate;
        }
        best_candidate->valid = false;
        // free is already true
        best_candidate->keeping = false;
        best_candidate->birth = 0;
        best_candidate->merged = true;

        remove_from_bucket(best_candidate);
        insert_into_buckets(best_candidate, best_candidate->grandparent, false);

        target_region = best_candidate;
    }
    assert(target_region);

    if (target_region->size > rounded_size) {
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
        target_region->keeping = false;
    }
    target_region->birth = target_region->grandparent->allocator->curr_time;
    target_region->merged = false;

    exit_trace("allocate_cl_region");

#ifdef VERBOSE
    if (target_region) {
        fprintf(stderr, "After trying to allocate %lu bytes, target_region=(%p "
                "%lu %lu)\n", rounded_size, target_region, target_region->offset,
                target_region->size);
        print_allocator(allocator, -1);
    }
#endif

    unlock_allocator(allocator);

    return target_region;
}

cl_allocator *init_allocator(cl_device_id dev, cl_context ctx, cl_command_queue cmd) {
    enter_trace("init_allocator");
    cl_ulong global_mem_size, max_alloc_size;
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_GLOBAL_MEM_SIZE,
                sizeof(global_mem_size), &global_mem_size, NULL));
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_MEM_ALLOC_SIZE,
                sizeof(max_alloc_size), &max_alloc_size, NULL));

    int nallocs = (global_mem_size + max_alloc_size - 1) / max_alloc_size;
    // int nallocs = 3;

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
        cl_mem mem;
        while (true) {
            mem = clCreateBuffer(ctx, CL_MEM_READ_WRITE, alloc_size, NULL, &err);
            CHECK(err);
            err = clEnqueueWriteBuffer(cmd, mem, CL_TRUE, 0, sizeof(err), &err, 0, NULL, NULL);
            if (err == CL_MEM_OBJECT_ALLOCATION_FAILURE) {
                assert(i == nallocs - 1);
                alloc_size -= (20 * 1024 * 1024);
                CHECK(clReleaseMemObject(mem));
            } else {
                CHECK(err);
                break;
            }
        }

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
        first_region->keeping = false;
        first_region->birth = 0;
        first_region->merged = false;
        first_region->cached = false;

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

        (allocator->allocs)[i].region_list_head = first_region;
        insert_into_buckets(first_region, allocator->allocs + i, false);
    }
    exit_trace("init_allocator");

    return allocator;
}

static void print_bucket(int bucket_lbl, cl_bucket *bucket) {
    if (bucket->head == NULL) return;

    fprintf(stderr, "             %d: ", bucket_lbl);
    cl_region *curr = bucket->head;
    while (curr) {
        fprintf(stderr, " (%p %lu %lu)", curr, curr->offset, curr->size);
        curr = curr->bucket_next;
    }
    fprintf(stderr, "\n");
}

void print_allocator(cl_allocator *allocator, int lbl) {
    fprintf(stderr, "allocator %d -> { %d allocs\n", lbl, allocator->nallocs);
    for (int i = 0; i < allocator->nallocs; i++) {
        cl_alloc *alloc = allocator->allocs + i;
        fprintf(stderr, "    alloc %d -> {\n", i);
        fprintf(stderr, "          size=%lu\n", alloc->size);
        fprintf(stderr, "          buckets = {\n");
        for (int b = 0; b < NBUCKETS; b++) {
            print_bucket(b, alloc->buckets + b);
        }
        fprintf(stderr, "            }\n");
        fprintf(stderr, "          keep_buckets = {\n");
        for (int b = 0; b < NBUCKETS; b++) {
            print_bucket(b, alloc->keep_buckets + b);
        }
        fprintf(stderr, "            }\n");
        fprintf(stderr, "          large_bucket =\n");
        print_bucket(-1, &alloc->large_bucket);
        fprintf(stderr, "          keep_large_bucket =\n");
        print_bucket(-1, &alloc->keep_large_bucket);
        fprintf(stderr, "      }\n");
    }
    fprintf(stderr, "  }\n");
}

void bump_time(cl_allocator *allocator) {
    enter_trace("bump_time");
    lock_allocator(allocator);
    allocator->curr_time += 1;
    unlock_allocator(allocator);
    exit_trace("bump_time");
}
