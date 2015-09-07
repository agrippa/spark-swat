#include "allocator.h"

#include <math.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

static void lock_alloc(cl_alloc *alloc) {
    ENTER_TRACE("lock_alloc");
    int perr = pthread_mutex_lock(&alloc->lock);
    ASSERT(perr == 0);
    EXIT_TRACE("lock_alloc");
}

static void unlock_alloc(cl_alloc *alloc) {
    ENTER_TRACE("unlock_alloc");
    int perr = pthread_mutex_unlock(&alloc->lock);
    ASSERT(perr == 0);
    EXIT_TRACE("unlock_alloc");
}

static void bucket_insert_after(cl_region *target, cl_region *to_insert,
        cl_bucket *bucket) {
    ENTER_TRACE("bucket_insert_after");
    ASSERT(target && to_insert);

    cl_region *next = target->bucket_next;
    target->bucket_next = to_insert;
    if (next) {
        next->bucket_prev = to_insert;
    } else {
        bucket->tail = to_insert;
    }
    to_insert->bucket_prev = target;
    to_insert->bucket_next = next;
    EXIT_TRACE("bucket_insert_after");
}

static void bucket_insert_before(cl_region *target, cl_region *to_insert,
        cl_bucket *bucket) {
    ENTER_TRACE("bucket_insert_before");
    ASSERT(target && to_insert);

    cl_region *prev = target->bucket_prev;
    target->bucket_prev = to_insert;
    if (prev) {
        prev->bucket_next = to_insert;
    } else {
        bucket->head = to_insert;
    }
    to_insert->bucket_next = target;
    to_insert->bucket_prev = prev;
    EXIT_TRACE("bucket_insert_before");
}

static void remove_from_bucket(cl_region *region) {
    ENTER_TRACE("remove_from_bucket");
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
    EXIT_TRACE("remove_from_bucket");
}

static void add_to_normal_bucket(cl_region *region, cl_bucket *bucket) {
    ENTER_TRACE("add_to_normal_bucket");
    ASSERT(region->parent == NULL);

    if (bucket->head == NULL) {
        // empty bucket
        ASSERT(bucket->tail == NULL);
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
    EXIT_TRACE("add_to_normal_bucket");
}

static void add_to_keep_bucket(cl_region *region, cl_bucket *bucket) {
    ENTER_TRACE("add_to_keep_bucket");
    ASSERT(region->parent == NULL);

    if (bucket->head == NULL) {
        // empty bucket
        ASSERT(bucket->tail == NULL);
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
    EXIT_TRACE("add_to_keep_bucket");
}

static void insert_into_buckets_helper(cl_region *region, cl_bucket *buckets,
        cl_bucket *large_bucket) {
    ENTER_TRACE("insert_into_buckets_helper");
    ASSERT(region->size >= MIN_ALLOC_SIZE);
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
    EXIT_TRACE("insert_into_buckets_helper");
}

static void insert_into_buckets(cl_region *region, cl_alloc *alloc, bool try_to_keep) {
    ENTER_TRACE("insert_into_buckets");
    region->keeping = try_to_keep;
    if (try_to_keep) {
        insert_into_buckets_helper(region, alloc->keep_buckets, &alloc->keep_large_bucket);
    } else {
        insert_into_buckets_helper(region, alloc->buckets, &alloc->large_bucket);
    }
    EXIT_TRACE("insert_into_buckets");
}

static cl_region *search_bucket(size_t rounded_size, cl_bucket *bucket) {
    ENTER_TRACE("search_bucket");
    cl_region *result = NULL;
    for (cl_region *r = bucket->head; r != NULL; r = r->bucket_next) {
        if (r->size >= rounded_size) {
            result = r;
            break;
        }
    }
    EXIT_TRACE("search_bucket");
    return result;
}

static cl_region *copy_cl_region(cl_region *input) {
    cl_region *copy = (cl_region *)malloc(sizeof(cl_region));
    CHECK_ALLOC(copy)
    memcpy(copy, input, sizeof(cl_region));
    copy->refs = 0;
    copy->parent = NULL;
    copy->bucket_next = NULL;
    copy->bucket_prev = NULL;
    copy->keeping = false; // can't be keeping because no one can have a reference
    copy->invalidated = false;
    return copy;
}

static cl_region *swap_out_for_copy(cl_region *input) {
    ASSERT(input->parent == NULL); // not in buckets
    cl_region *copy = copy_cl_region(input);
    if (copy->prev) {
        copy->prev->next = copy;
    }
    if (copy->next) {
        copy->next->prev = copy;
    }
    if (copy->grandparent->region_list_head == input) {
        copy->grandparent->region_list_head = copy;
    }
    if (input->keeping) {
        input->invalidated = true;
    } else {
        free(input);
    }
    return copy;
}

/*
 * On a split there's never any reason to update alloc->region_list_head,
 * because the split always splits off a region from the back of target (and not
 * the front), this can't change the head of the list.
 */
static void split(cl_region *target, size_t first_partition_size,
        cl_region **first_out, cl_region **second_out) {
    ENTER_TRACE("split");
    ASSERT(target->refs == 0);

    cl_alloc *alloc = target->grandparent;

    cl_region *lower_region = copy_cl_region(target);
    cl_region *upper_region = copy_cl_region(target);

    lower_region->sub_mem = NULL;
    upper_region->sub_mem = NULL;

    upper_region->offset += first_partition_size;

    lower_region->size = first_partition_size;
    upper_region->size -= first_partition_size;

    lower_region->prev = target->prev;
    lower_region->next = upper_region;
    upper_region->next = target->next;
    upper_region->prev = lower_region;
    if (target->prev) {
        target->prev->next = lower_region;
    }
    if (target->next) {
        target->next->prev = upper_region;
    }

    if (alloc->region_list_head == target) {
        ASSERT(lower_region->prev == NULL);
        alloc->region_list_head = lower_region;
    }

    remove_from_bucket(target);

    if (target->keeping) {
        target->invalidated = true;
    } else {
        free(target);
    }

    insert_into_buckets(lower_region, alloc, false);
    insert_into_buckets(upper_region, alloc, false);

    *first_out = lower_region;
    *second_out = upper_region;
    EXIT_TRACE("split");
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
#ifdef VERBOSE
        fprintf(stderr, "  bucket=%d rounded_size=%lu min_bucket_incl=%lu "
                "match? %d\n", b, rounded_size, BUCKET_MIN_SIZE_INCL(b),
                (BUCKET_MIN_SIZE_INCL(b) >= rounded_size));
#endif
        /*
         * Look at all buckets that have allocations larger than or equal to
         * rounded_size.
         */
        if (BUCKET_MIN_SIZE_INCL(b) >= rounded_size) {
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
                break;
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
    ENTER_TRACE("free_cl_region");

    lock_alloc(to_free->grandparent);

    ASSERT(to_free->refs > 0);
    ASSERT(to_free->sub_mem);
    ASSERT(!to_free->invalidated);
    ASSERT(to_free->parent == NULL); // not in a bucket
    to_free->refs -= 1;

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
        ASSERT(next == NULL || next->prev == to_free);
        ASSERT(prev == NULL || prev->next == to_free);
        ASSERT(next == NULL || next->offset == to_free->offset + to_free->size);
        ASSERT(prev == NULL || to_free->offset == prev->offset + prev->size);
        to_free->keeping = try_to_keep;
        if (!try_to_keep) to_free->birth = 0;

        if (!try_to_keep && (next && next->refs == 0 && !next->keeping) &&
                (prev && prev->refs == 0 && !prev->keeping)) {
            /*
             * Merge with next and prev if neither is 1) free, or 2) an
             * allocation we're trying to hold on to as long as possible.
             */
#ifdef VERBOSE
            fprintf(stderr, "Merging prev=(%p offset=%lu size=%lu) and "
                    "next=(%p offset=%lu size=%lu) for to_free=(%p offset=%lu "
                    "size=%lu)\n", prev, prev->offset, prev->size, next,
                    next->offset, next->size, to_free, to_free->offset,
                    to_free->size);
#endif
            prev->sub_mem = NULL;
            prev->size += to_free->size + next->size;
            prev->next = next->next;
            if (prev->next) {
                prev->next->prev = prev;
            }
#ifdef VERBOSE
            fprintf(stderr, "  prev=(%p offset=%lu size=%lu)\n", prev,
                    prev->offset, prev->size);
#endif
            remove_from_bucket(next);
            free(next);
            free(to_free);
            remove_from_bucket(prev);
            insert_into_buckets(prev, prev->grandparent, false);
        } else if (!try_to_keep && next && next->refs == 0 && !next->keeping) {
            // Merge with just next
#ifdef VERBOSE
            fprintf(stderr, "Merging next=(%p offset=%lu size=%lu) for "
                    "to_free=(%p offset=%lu size=%lu)\n", next, next->offset,
                    next->size, to_free, to_free->offset, to_free->size);
#endif

            to_free->sub_mem = NULL;
            to_free->size += next->size;
            to_free->next = next->next;
            if (to_free->next) {
                to_free->next->prev = to_free;
            }
#ifdef VERBOSE
            fprintf(stderr, "  to_free=(%p offset=%lu size=%lu)\n", to_free,
                    to_free->offset, to_free->size);
#endif
            remove_from_bucket(next);
            free(next);
            insert_into_buckets(to_free, to_free->grandparent, false);
        } else if (!try_to_keep && prev && prev->refs == 0 && !prev->keeping) {
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

            prev->sub_mem = NULL;
            free(to_free);
            remove_from_bucket(prev);
            insert_into_buckets(prev, prev->grandparent, false);
        } else {
            insert_into_buckets(to_free, to_free->grandparent, try_to_keep);
            to_free->keeping = try_to_keep;
        }
#ifdef VERBOSE
        fprintf(stderr, "After freeing (%p %lu %lu)\n", to_free,
                to_free->offset, to_free->size);
        print_allocator(to_free->grandparent->allocator, -1);
#endif

    }

    bool return_value = (to_free->refs == 0);

    unlock_alloc(to_free->grandparent);

    EXIT_TRACE("free_cl_region");
    return return_value;
}

bool re_allocate_cl_region(cl_region *target_region, int target_device) {
    ENTER_TRACE("re_allocate_cl_region");

    if (target_region->invalidated) {
        free(target_region);
        EXIT_TRACE("re_allocate_cl_region");
        return false;
    }

    lock_alloc(target_region->grandparent);

    /*
     * For now, it is the responsibility of the higher layers to track which
     * device they are associated with and only re-allocated items on that
     * device. We simply enforce that restriction here.
     */
    ASSERT(GET_DEVICE_FOR(target_region) == target_device);

    /*
     * keeping may be false if this is a RDD/broadcast that hasn't been freed
     * yet, in which case it must have some refs. This isn't a strong check.
     */
    ASSERT(target_region->keeping || target_region->refs > 0);
    ASSERT(!target_region->invalidated);

    if (target_region->refs == 0) {
#ifdef VERBOSE
        fprintf(stderr, "Before Re-Allocating (%p %lu %lu)\n", target_region,
                target_region->offset, target_region->size);
        print_allocator(target_region->grandparent->allocator, -1);
#endif
        remove_from_bucket(target_region);
        ASSERT(target_region->sub_mem);
        target_region->refs = 1;
#ifdef VERBOSE
        fprintf(stderr, "After Re-Allocating (%p %lu %lu)\n", target_region,
                target_region->offset, target_region->size);
        print_allocator(target_region->grandparent->allocator, -1);
#endif

    } else {
        target_region->refs += 1;
    }
    target_region->birth = target_region->grandparent->curr_time;

    unlock_alloc(target_region->grandparent);

    EXIT_TRACE("re_allocate_cl_region");

    return true;
}

cl_region *allocate_cl_region(size_t size, cl_allocator *allocator) {
    ENTER_TRACE("allocate_cl_region");
    ASSERT(allocator);
    ASSERT(size > 0);

    // size_t rounded_size = size;
    size_t rounded_size = size + (allocator->address_align -
            (size % allocator->address_align));
    // size_t rounded_size = pow(2, ceil(log(size)/log(2)));

#ifdef VERBOSE
    fprintf(stderr, "Allocating %lu (actual=%lu) bytes, allocator state "
            "beforehand:\n", rounded_size, size);
    print_allocator(allocator, -1);
#endif

    cl_region *target_region = NULL;

    // First look in the normal buckets
    for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
        cl_alloc *alloc = allocator->allocs + a;

        lock_alloc(alloc);
        target_region = find_matching_region_in_alloc(rounded_size, alloc->buckets);
        if (target_region == NULL) {
            unlock_alloc(alloc);
        }
    }
#ifdef VERBOSE
    fprintf(stderr, "Got target_region=%p from searching buckets for region of "
            "size %lu\n", target_region, rounded_size);
#endif

    // Then look in the large normal buckets
    if (target_region == NULL) {
        for (int a = 0; a < allocator->nallocs && target_region == NULL; a++) {
            cl_alloc *alloc = allocator->allocs + a;

            lock_alloc(alloc);
            target_region = search_bucket(rounded_size, &alloc->large_bucket);
            if (target_region == NULL) {
                unlock_alloc(alloc);
            }
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

            lock_alloc(alloc);
            target_region = find_matching_region_in_alloc(rounded_size, alloc->keep_buckets);
            if (target_region == NULL) {
                unlock_alloc(alloc);
            }
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

            lock_alloc(alloc);
            target_region = search_bucket(rounded_size, &alloc->keep_large_bucket);
            if (target_region == NULL) {
                unlock_alloc(alloc);
            }
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
            lock_alloc(alloc);

            cl_region *curr = alloc->region_list_head;

            while (curr) {
                size_t acc_bytes = curr->size;
                long youngest = curr->birth;
                int count_successors = 0;
                cl_region *succ = curr->next;

                if (curr->refs > 0) { // not free
                    curr = curr->next;
                    continue;
                }

#ifdef VERBOSE
                fprintf(stderr, "  Starting search from (%p offset=%lu size=%lu)\n",
                        curr, curr->offset, curr->size);
#endif

                while (succ && succ->refs == 0 && acc_bytes < size) {
                    acc_bytes += succ->size;
                    youngest = (succ->birth > youngest ? succ->birth : youngest);
                    count_successors++;
                    succ = succ->next;
                }

                /*
                 * Look for the group with the oldest, youngest member
                 */
                if (acc_bytes >= size && (best_candidate == NULL ||
                            youngest < best_candidate_birth)) {
                    best_candidate = curr;
                    best_candidate_successors = count_successors;
                    best_candidate_birth = youngest;
                }

                curr = curr->next;
            }

            if (best_candidate) {
                // Break out with the alloc lock still held
                break;
            }
            unlock_alloc(alloc);
        }

        ASSERT(best_candidate);
#ifdef VERBOSE
        fprintf(stderr, "best_candidate=(%p offset=%lu size=%lu refs=%d), "
                "successors=%d\n", best_candidate, best_candidate->offset,
                best_candidate->size, best_candidate->refs, best_candidate_successors);
        if (best_candidate->keeping) {
            fprintf(stderr, "Evicting %p offset=%lu size=%lu\n", best_candidate,
                    best_candidate->offset, best_candidate->size);
        }
#endif
        cl_region *new_region = (cl_region *)malloc(sizeof(cl_region));
        CHECK_ALLOC(new_region)
        memcpy(new_region, best_candidate, sizeof(cl_region));

        cl_region *succ = best_candidate->next;
        int count_succ = 0;
        while (count_succ < best_candidate_successors) {

            new_region->size += succ->size;
            // no need to handle parent
            // grandparent unchanged
            // bucket_next, bucket_prev will be handled later
            new_region->next = succ->next;  // eventually the last successor->next
            // prev remains the same
            // refs is already 0
#ifdef VERBOSE
            if (succ->keeping) {
                fprintf(stderr, "Evicting %p offset=%lu size=%lu\n", succ,
                        succ->offset, succ->size);
            }
#endif

            count_succ++;
            cl_region *next = succ->next;

            remove_from_bucket(succ);
            /*
             * keeping is set in free_cl_region when an item is finally freed, so
             * we know it must be set to the actual value here because we are
             * only dealing with freed objects.
             */
            if (succ->keeping) {
                succ->next = NULL;
                succ->prev = NULL;
                succ->invalidated = true;
            } else {
                free(succ);
            }
            succ = next;
        }

        if (new_region->next) {
            new_region->next->prev = new_region;
        }
        if (new_region->prev) {
            new_region->prev->next = new_region;
        }
        if (new_region->grandparent->region_list_head == best_candidate) {
            new_region->grandparent->region_list_head = new_region;
        }
        new_region->sub_mem = NULL;
        new_region->refs = 0;
        new_region->keeping = false;
        new_region->birth = 0;
        new_region->invalidated = false;

        remove_from_bucket(best_candidate);
        new_region->bucket_next = NULL;
        new_region->bucket_prev = NULL;
        new_region->parent = NULL;
        insert_into_buckets(new_region, new_region->grandparent, false);

        if (best_candidate->keeping) {
            best_candidate->invalidated = true;
            best_candidate->next = NULL;
            best_candidate->prev = NULL;
        } else {
            free(best_candidate);
        }

        target_region = new_region;
    }
    ASSERT(target_region);

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
    ASSERT(target_region->refs == 0 && target_region->refs == 0);

    cl_region *copy = swap_out_for_copy(target_region);
    copy->refs = 1;
    if (copy->sub_mem == NULL) {
#ifdef OPENCL_ALLOCATOR
        int err;
        cl_buffer_region sub_region;
        sub_region.origin = copy->offset;
        sub_region.size = copy->size;
        copy->sub_mem = clCreateSubBuffer(alloc->mem, 0,
                CL_BUFFER_CREATE_TYPE_REGION, &sub_region, &err);
        CHECK(err);
#else
        copy->sub_mem = alloc->mem + copy->offset;
#endif
        copy->keeping = false;
    }
    copy->birth = copy->grandparent->curr_time;

    EXIT_TRACE("allocate_cl_region");

#ifdef VERBOSE
    if (copy) {
        fprintf(stderr, "After trying to allocate %lu bytes, copy=(%p "
                "%lu %lu)\n", rounded_size, copy, copy->offset,
                copy->size);
        print_allocator(allocator, -1);
    }
#endif

    unlock_alloc(alloc);

    return copy;
}

#ifdef OPENCL_ALLOCATOR
cl_allocator *init_allocator(cl_device_id dev, int device_index, cl_context ctx,
        cl_command_queue cmd)
#else
cl_allocator *init_allocator(int device_index)
#endif
{
    ENTER_TRACE("init_allocator");

    unsigned int address_align;
#ifdef OPENCL_ALLOCATOR
    cl_ulong global_mem_size, max_alloc_size;
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_GLOBAL_MEM_SIZE,
                sizeof(global_mem_size), &global_mem_size, NULL));
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_MEM_ALLOC_SIZE,
                sizeof(max_alloc_size), &max_alloc_size, NULL));

    int nallocs = (global_mem_size + max_alloc_size - 1) / max_alloc_size;
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_MEM_BASE_ADDR_ALIGN,
                sizeof(address_align), &address_align, NULL));
#else
    int nallocs = 1;
    address_align = 8;
    size_t max_alloc_size, global_mem_size;
    struct cudaDeviceProp props;
    CHECK(cudaGetDeviceProperties(&props, device_index));
    max_alloc_size = global_mem_size = props.totalGlobalMem;
#endif


    cl_allocator *allocator = (cl_allocator *)malloc(sizeof(cl_allocator));
    CHECK_ALLOC(allocator)
    allocator->nallocs = nallocs;
    allocator->allocs = (cl_alloc *)malloc(nallocs * sizeof(cl_alloc));
    CHECK_ALLOC(allocator->allocs)
    allocator->address_align = address_align;
    allocator->device_index = device_index;

    for (int i = 0; i < nallocs; i++) {
        size_t alloc_size = max_alloc_size;
        size_t leftover = global_mem_size - (i * max_alloc_size);
        if (leftover < alloc_size) {
            alloc_size = leftover;
        }

#ifdef OPENCL_ALLOCATOR
        int err;
        cl_mem mem;
#else
        cudaError_t err;
        char *mem;
#endif
        while (true) {
#ifdef OPENCL_ALLOCATOR
            mem = clCreateBuffer(ctx, CL_MEM_READ_WRITE, alloc_size, NULL,
                    &err);
            CHECK(err);
            err = clEnqueueWriteBuffer(cmd, mem, CL_TRUE, 0, sizeof(err), &err, 0, NULL, NULL);
            if (err == CL_MEM_OBJECT_ALLOCATION_FAILURE) {
                ASSERT(i == nallocs - 1);
                alloc_size -= (20 * 1024 * 1024);
                CHECK(clReleaseMemObject(mem));
            } else {
                CHECK(err);
                break;
            }
#else
            err = cudaMalloc((void **)&mem, alloc_size);
            if (err == cudaErrorMemoryAllocation) {
                ASSERT(i == nallocs - 1);
                alloc_size -= (20 * 1024 * 1024);
            } else if (err != cudaSuccess) {
                CHECK(err);
            }
#endif
        }

        (allocator->allocs)[i].mem = mem;
        (allocator->allocs)[i].size = alloc_size;
        (allocator->allocs)[i].allocator = allocator;

        int perr = pthread_mutex_init(&(allocator->allocs)[i].lock, NULL);
        ASSERT(perr == 0);

        cl_region *first_region = (cl_region *)malloc(sizeof(cl_region));
        CHECK_ALLOC(first_region)
        first_region->sub_mem = NULL;
        first_region->offset = 0;
        first_region->size = alloc_size;
        first_region->parent = NULL;
        first_region->grandparent = allocator->allocs + i;
        first_region->bucket_next = NULL;
        first_region->bucket_prev = NULL;
        first_region->next = NULL;
        first_region->prev = NULL;
        first_region->refs = 0;
        first_region->keeping = false;
        first_region->birth = 0;
        first_region->invalidated = false;

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
    EXIT_TRACE("init_allocator");

    return allocator;
}

static size_t print_bucket(int bucket_lbl, cl_bucket *bucket) {
    if (bucket->head == NULL) return 0;
    size_t count_bytes = 0;

    fprintf(stderr, "             %d: ", bucket_lbl);
    cl_region *curr = bucket->head;
    while (curr) {
        ASSERT(curr->refs == 0);
        fprintf(stderr, " (%p off=%lu size=%lu)", curr, curr->offset, curr->size);
        count_bytes += curr->size;
        curr = curr->bucket_next;
    }
    fprintf(stderr, "\n");
    return count_bytes;
}

void print_allocator(cl_allocator *allocator, int lbl) {
    fprintf(stderr, "allocator %d -> { %d allocs\n", lbl, allocator->nallocs);
    for (int i = 0; i < allocator->nallocs; i++) {
        cl_alloc *alloc = allocator->allocs + i;
        size_t free_bytes = 0;
        fprintf(stderr, "    alloc %d -> {\n", i);
        fprintf(stderr, "          size=%lu\n", alloc->size);
        fprintf(stderr, "          buckets = {\n");
        for (int b = 0; b < NBUCKETS; b++) {
            free_bytes += print_bucket(b, alloc->buckets + b);
        }
        fprintf(stderr, "            }\n");
        fprintf(stderr, "          keep_buckets = {\n");
        for (int b = 0; b < NBUCKETS; b++) {
            free_bytes += print_bucket(b, alloc->keep_buckets + b);
        }
        fprintf(stderr, "            }\n");
        fprintf(stderr, "          large_bucket =\n");
        free_bytes += print_bucket(-1, &alloc->large_bucket);
        fprintf(stderr, "          keep_large_bucket =\n");
        free_bytes += print_bucket(-1, &alloc->keep_large_bucket);
        fprintf(stderr, "          free_bytes = %lu\n", free_bytes);
        fprintf(stderr, "      }\n");
    }
    fprintf(stderr, "  }\n");
}

void bump_time(cl_allocator *allocator) {
    ENTER_TRACE("bump_time");
    for (int i = 0; i < allocator->nallocs; i++) {
        cl_alloc *curr = allocator->allocs + i;
        lock_alloc(curr);
        curr->curr_time += 1;
        unlock_alloc(curr);
    }
    EXIT_TRACE("bump_time");
}
