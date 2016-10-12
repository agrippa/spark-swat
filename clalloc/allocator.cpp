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

#include "allocator.h"

#include <math.h>
#include <string.h>

#ifdef PROFILE
static volatile unsigned long long acc_init_time = 0ULL;
static volatile unsigned long long acc_realloc_time = 0ULL;
static volatile unsigned long long acc_free_time = 0ULL;
static volatile unsigned long long acc_alloc_time = 0ULL;
#endif

#if defined(PROFILE_LOCKS) || defined(PROFILE)
unsigned long long get_clock_gettime_ns() {
    struct timespec t ={0,0};
    clock_gettime(CLOCK_MONOTONIC, &t);
    unsigned long long s = 1000000000ULL * (unsigned long long)t.tv_sec;
    return (unsigned long long)t.tv_nsec + s;
}
#endif

/*
 * Lock a selected alloc to prevent concurrent access to any of its member
 * regions.
 */
static void lock_alloc(cl_alloc *alloc) {
    ENTER_TRACE("lock_alloc");
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    const int perr = pthread_mutex_lock(&alloc->lock);
    if (perr != 0) {
        fprintf(stderr, "lock_alloc: perr=%d\n", perr);
        exit(1);
    }
#ifdef PROFILE_LOCKS
    alloc->contention += (get_clock_gettime_ns() - start);
#endif

    ASSERT(perr == 0);
    EXIT_TRACE("lock_alloc");
}

static void unlock_alloc(cl_alloc *alloc) {
    ENTER_TRACE("unlock_alloc");
    const int perr = pthread_mutex_unlock(&alloc->lock);
    if (perr != 0) {
        fprintf(stderr, "unlock_alloc: perr=%d\n", perr);
        exit(1);
    }
    EXIT_TRACE("unlock_alloc");
}

/*
 * Insert region to_insert after region target inside of bucket.
 */
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

/*
 * Insert region to_insert after region target inside of bucket
 */
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

/*
 * Remove the provided region from its parent bucket
 */
static void remove_from_bucket(cl_region *region) {
    ENTER_TRACE("remove_from_bucket");
    cl_bucket *bucket = region->parent;

    if (bucket->head == region && bucket->tail == region) {
        // Single element bucket
        bucket->head = bucket->tail = NULL;
    } else if (bucket->head == region) {
        // Region is first element of bucket, >1 elements
        cl_region *new_head = bucket->head->bucket_next;
        new_head->bucket_prev = NULL;
        bucket->head = new_head;
    } else if (bucket->tail == region) {
        // Region is last element of bucket, >1 elements
        cl_region *new_tail = bucket->tail->bucket_prev;
        new_tail->bucket_next = NULL;
        bucket->tail = new_tail;
    } else {
        // General case, interior element in bucket
        cl_region *prev = region->bucket_prev;
        cl_region *next = region->bucket_next;
        next->bucket_prev = prev;
        prev->bucket_next = next;
    }
    region->bucket_next = region->bucket_prev = NULL;
    region->parent = NULL;
    EXIT_TRACE("remove_from_bucket");
}

/*
 * Insert region in the provided bucket, keeping the bucket sorted in ascending
 * region size order.
 */
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

/*
 * Add the provided region to the provided bucket, ordered first by how old this
 * region is, and then by region size (ascending).
 */
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

/*
 * Given a region, a pre-allocated space, and a conditional indicating if we'd
 * like to try and keep this data persistent on the accelerator, insert the
 * region into the appropriate free buckets in alloc. This function assumes that
 * any caller has done the necessary locking of alloc.
 */
static void insert_into_buckets(cl_region *region, cl_alloc *alloc,
        bool try_to_keep) {
    ENTER_TRACE("insert_into_buckets");
    region->keeping = try_to_keep;
    if (try_to_keep) {
        insert_into_buckets_helper(region, alloc->keep_buckets,
                &alloc->keep_large_bucket);
    } else {
        insert_into_buckets_helper(region, alloc->buckets, &alloc->large_bucket);
    }
    EXIT_TRACE("insert_into_buckets");
}

/*
 * Given a bucket and a region size to search for, return the first region whose
 * size is >= the target size.
 */
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
    CHECK_ALLOC(copy);
    memcpy(copy, input, sizeof(cl_region));
    copy->refs = 0; // no one can reference a newly created region object
    copy->parent = NULL; // not in a bucket yet
    copy->bucket_next = NULL;
    copy->bucket_prev = NULL;
    copy->keeping = false; // can't be keeping because no one can have a reference
    copy->invalidated = false;
    return copy;
}

/*
 * Swap out a given region for an identical copy. input cannot be in any
 * buckets. If input is marked keeping, then mark it invalidated for the host
 * program to later discover on re-allocation. Otherwise, free it immediately.
 */
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

    ASSERT(input->refs == 0); // not in active use
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

    lower_region->sub_mem = (cl_mem)NULL;
    upper_region->sub_mem = (cl_mem)NULL;

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

    ASSERT(lower_region->size >= MIN_ALLOC_SIZE);
    ASSERT(upper_region->size >= MIN_ALLOC_SIZE);
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

/*
 * Search the provided buckets all belonging to the same alloc (which has
 * already been locked) for any region which is at least of length rounded_size.
 */
static cl_region *find_matching_region_in_alloc(size_t rounded_size,
        cl_bucket *buckets) {
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

size_t count_free_bytes(cl_allocator *allocator) {
    size_t count = 0;
    for (int a = 0; a < allocator->nallocs; a++) {
        cl_alloc *alloc = allocator->allocs + a;
        lock_alloc(alloc);
    }
    for (int a = 0; a < allocator->nallocs; a++) {
        cl_alloc *alloc = allocator->allocs + a;
        count += alloc->free_bytes;
    }
    for (int a = 0; a < allocator->nallocs; a++) {
        cl_alloc *alloc = allocator->allocs + a;
        unlock_alloc(alloc);
    }
    return count;
}

/*
 * free_cl_region always merges later ranges into earlier ranges, so no need to
 * update region_list_head. Returns true if the region was actually freed, false
 * if its ref count was just decremented.
 */
bool free_cl_region(cl_region *to_free, bool try_to_keep) {
#ifdef PROFILE
    const unsigned long long start = get_clock_gettime_ns();
#endif
    ENTER_TRACE("free_cl_region");

    cl_alloc *to_free_grandparent = to_free->grandparent;
    lock_alloc(to_free_grandparent);

    ASSERT(to_free->refs > 0);
    ASSERT(to_free->sub_mem);
    ASSERT(!to_free->invalidated);
    ASSERT(to_free->parent == NULL); // not in a bucket
    to_free->refs -= 1;

    if (to_free->refs == 0) {
        /*
         * If there are no longer any refs to this region, we can actually free
         * up the space for use by other allocations. If this allocation is
         * marked try_to_keep, then we do our best to keep its contents
         * persistent in memory for as long as possible.
         */
        cl_region *next = to_free->next;
        cl_region *prev = to_free->prev;
#ifdef VERBOSE
        fprintf(stderr, "Before freeing (%p mem=%p offset=%lu size=%lu) on "
                "device %d\n", to_free, to_free->sub_mem, to_free->offset,
                to_free->size, to_free->grandparent->allocator->device_index);
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
        // Check links and relations between neighboring regions
        ASSERT(next == NULL || next->prev == to_free);
        ASSERT(prev == NULL || prev->next == to_free);
        ASSERT(next == NULL || next->offset == to_free->offset + to_free->size);
        ASSERT(prev == NULL || to_free->offset == prev->offset + prev->size);

        to_free->keeping = try_to_keep;

#ifdef VERBOSE
        fprintf(stderr, "Freeing region=%p size=%lu parent free "
                "bytes=%lu->%lu\n", to_free, to_free->size,
                to_free->grandparent->free_bytes,
                to_free->grandparent->free_bytes + to_free->size);
#endif

        // Keep metrics on how many bytes are free to help with leak detection
        to_free->grandparent->free_bytes += to_free->size;

        if (!try_to_keep && (next && next->refs == 0 && !next->keeping) &&
                (prev && prev->refs == 0 && !prev->keeping)) {
            /*
             * Merge with next and prev if neither is 1) free, or 2) an
             * allocation we're trying to hold on to as long as possible, and
             * we're not trying to keep this one around either.
             */
#ifdef VERBOSE
            fprintf(stderr, "Merging prev=(%p offset=%lu size=%lu) and "
                    "next=(%p offset=%lu size=%lu) for to_free=(%p offset=%lu "
                    "size=%lu)\n", prev, prev->offset, prev->size, next,
                    next->offset, next->size, to_free, to_free->offset,
                    to_free->size);
#endif
            prev->sub_mem = (cl_mem)NULL;
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
            ASSERT(prev->size >= MIN_ALLOC_SIZE);
            insert_into_buckets(prev, prev->grandparent, false);
        } else if (!try_to_keep && next && next->refs == 0 && !next->keeping) {
            // Merge with just next
#ifdef VERBOSE
            fprintf(stderr, "Merging next=(%p offset=%lu size=%lu) for "
                    "to_free=(%p offset=%lu size=%lu)\n", next, next->offset,
                    next->size, to_free, to_free->offset, to_free->size);
#endif

            to_free->sub_mem = (cl_mem)NULL;
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
            ASSERT(to_free->size >= MIN_ALLOC_SIZE);
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

            prev->sub_mem = (cl_mem)NULL;
            free(to_free);
            remove_from_bucket(prev);
            ASSERT(prev->size >= MIN_ALLOC_SIZE);
            insert_into_buckets(prev, prev->grandparent, false);
        } else {
            /*
             * We can't do any merging with next or previous, so just add the
             * current region to the best matching bucket.
             */
            ASSERT(to_free->size >= MIN_ALLOC_SIZE);
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

    unlock_alloc(to_free_grandparent);

    EXIT_TRACE("free_cl_region");
#ifdef PROFILE
    __sync_fetch_and_add(&acc_free_time, get_clock_gettime_ns() - start);
#endif
    return return_value;
}

void *fetch_pinned(cl_region *region) {
    cl_alloc *grandparent = region->grandparent;
    return (void *)(grandparent->pinned + region->offset);
}

/*
 * Fetch a region of the specified size from the specified allocator.
 */
cl_region *allocate_cl_region(size_t size, cl_allocator *allocator,
        void (*callback)(void *), void *user_data) {
#ifdef PROFILE
    const unsigned long long start = get_clock_gettime_ns();
#endif
    ENTER_TRACE("allocate_cl_region");
    ASSERT(allocator);

    size_t rounded_size = size + (allocator->address_align -
            (size % allocator->address_align));

#ifdef VERBOSE
    fprintf(stderr, "Allocating %lu (actual=%lu) bytes from device %d, "
            "allocator state beforehand:\n", rounded_size, size,
            allocator->device_index);
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
         * keep regions (either that or memory is so segmented by actively used
         * allocations that this allocation is doomed).
         */
        cl_region *best_candidate = NULL;
        int best_candidate_successors = -1;
        long best_candidate_birth = -1;

        for (int a = 0; a < allocator->nallocs; a++) {
            cl_alloc *alloc = allocator->allocs + a;
            lock_alloc(alloc);

            cl_region *curr = alloc->region_list_head;

            /*
             * Iterate over this whole alloc in address order, through next and
             * prev (rather than bucket_next and bucket_prev).
             */
            while (curr) {

                // Skip if not free
                if (curr->refs > 0) {
                    curr = curr->next;
                    continue;
                }

                /*
                 * Starting at curr, try to see if we can accumulate together
                 * enough free regions to satisfy this memory allocation.
                 */
                size_t acc_bytes = curr->size;
                long youngest = curr->birth;
                int count_successors = 0;
                cl_region *succ = curr->next;

#ifdef VERBOSE
                fprintf(stderr, "  Starting search from (%p offset=%lu "
                        "size=%lu)\n", curr, curr->offset, curr->size);
#endif
                while (succ && succ->refs == 0 && acc_bytes < size) {
                    acc_bytes += succ->size;
                    count_successors++;
                    youngest = (succ->birth > youngest ? succ->birth : youngest);
                    succ = succ->next;
                }

                /*
                 * Look for the group with the oldest young member
                 */
                if (acc_bytes >= size && (best_candidate == NULL ||
                            youngest < best_candidate_birth)) {
                    best_candidate = curr;
                    best_candidate_successors = count_successors;
                    best_candidate_birth = youngest;
                }

                if (succ && succ->refs > 0) {
                    /*
                     * As a simple optimization, if we've run into an allocated
                     * region before satisfying this allocation we know that
                     * traversing the other regions between curr and succ is
                     * pointless, so just skip past this barrier.
                     */
                    curr = succ->next;
                } else {
                    curr = curr->next;
                }
            }

            if (best_candidate) {
                // Break out with the alloc lock still held
                break;
            }
            unlock_alloc(alloc);
        }

        if (best_candidate == NULL) {
            /*
             * If every attempt we make fails to successfully allocate data,
             * call the user provided call back with the user provided data to
             * notify the host program and return NULL.
             *
             * TODO what is the purpose of callback?
             */
            if (callback) (*callback)(user_data);
            return NULL;
        }
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
        CHECK_ALLOC(new_region);
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
             * we know it must be set to the correct value here because we are
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
        new_region->sub_mem = (cl_mem)NULL;
        new_region->refs = 0;
        new_region->keeping = false;
        new_region->birth = 0;
        new_region->invalidated = false;

        remove_from_bucket(best_candidate);
        new_region->bucket_next = NULL;
        new_region->bucket_prev = NULL;
        new_region->parent = NULL;
        ASSERT(new_region->size >= MIN_ALLOC_SIZE);
        insert_into_buckets(new_region, new_region->grandparent, false);

        if (best_candidate->keeping) {
            best_candidate->next = NULL;
            best_candidate->prev = NULL;
            best_candidate->invalidated = true;
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
    ASSERT(target_region->refs == 0);

    /*
     * Swap out for a completely new object to ensure that every allocation
     * retrieves new state, keeps a more consistent model.
     */
    cl_region *copy = swap_out_for_copy(target_region);
    copy->refs = 1;
    if (copy->sub_mem == (cl_mem)NULL) {
#ifdef USE_CUDA
        copy->sub_mem = alloc->mem + copy->offset;
#else
        int err;
        cl_buffer_region sub_region;
        sub_region.origin = copy->offset;
        sub_region.size = copy->size;
        copy->sub_mem = clCreateSubBuffer(alloc->mem, 0,
                CL_BUFFER_CREATE_TYPE_REGION, &sub_region, &err);
        CHECK(err);
#endif
        copy->keeping = false;
    }
    copy->birth = copy->grandparent->curr_time;
#ifdef VERBOSE
    fprintf(stderr, "Allocating region=%p mem=%p offset=%lu size=%lu parent "
            "free bytes=%lu->%lu on device %d\n", copy, copy->sub_mem, copy->offset,
            copy->size, copy->grandparent->free_bytes,
            copy->grandparent->free_bytes - copy->size, allocator->device_index);
#endif
    copy->grandparent->free_bytes -= copy->size;

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
#ifdef PROFILE
    __sync_fetch_and_add(&acc_alloc_time, get_clock_gettime_ns() - start);
#endif

    return copy;
}

/*
 * re_allocate_cl_region is intended only for use with allocations that have
 * been marked as keeping, i.e. things that the allocator will do its best to
 * keep persistent on the device even if no one actively has a handle to it.
 *
 * However, it will also work for regions that are allocated and shared among
 * multiple entities as long as at least one is always holding it. However, if
 * this is the use case and all entities release their handle, then another
 * comes along and tries to re-allocate through a stale handle, this method will
 * fail.
 *
 * Therefore, if you want to use re_allocate_cl_region to deduplicate read-only
 * data you must either 1) ensure that freeing these regions always sets
 * try_to_keep (which may lead to memory fragmentation), or 2) ensure there is
 * always at least one live reference.
 */
bool re_allocate_cl_region(cl_region *target_region, int target_device) {
#ifdef PROFILE
    const unsigned long long start = get_clock_gettime_ns();
#endif
    ENTER_TRACE("re_allocate_cl_region");

    cl_alloc *alloc = target_region->grandparent;
    lock_alloc(alloc);

    if (target_region->invalidated) {
#ifdef VERBOSE
        fprintf(stderr, "Cleaning up target_region=%p because invalidated\n",
                target_region);
#endif
        free(target_region);
        unlock_alloc(alloc);
        EXIT_TRACE("re_allocate_cl_region");
#ifdef PROFILE
    __sync_fetch_and_add(&acc_realloc_time, get_clock_gettime_ns() - start);
#endif
        return false;
    }

#ifdef VERBOSE
    fprintf(stderr, "re_allocate_cl_region: target_region=%p target_device=%d "
            "target_region->keeping=%d target_region->refs=%d "
            "target_region->invalidated=%d GET_DEVICE_FOR(target_region)=%d\n",
            target_region, target_device, target_region->keeping,
            target_region->refs, target_region->invalidated,
            GET_DEVICE_FOR(target_region));
#endif

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

#ifdef VERBOSE
        fprintf(stderr, "Re-allocating region=%p size=%lu parent free "
                "bytes=%lu->%lu\n", target_region, target_region->size,
                target_region->grandparent->free_bytes,
                target_region->grandparent->free_bytes - target_region->size);
#endif
        target_region->grandparent->free_bytes -= target_region->size;
    } else {
        target_region->refs += 1;
    }
    target_region->birth = target_region->grandparent->curr_time;

    unlock_alloc(alloc);

    EXIT_TRACE("re_allocate_cl_region");

#ifdef PROFILE
    __sync_fetch_and_add(&acc_realloc_time, get_clock_gettime_ns() - start);
#endif
    return true;
}

#ifdef USE_CUDA
cl_allocator *init_allocator(CUcontext ctx)
#else
cl_allocator *init_allocator(cl_device_id dev, int device_index,
        cl_mem_flags alloc_flags, size_t limit_size, cl_context ctx,
        cl_command_queue cmd)
#endif
{
    ENTER_TRACE("init_allocator");
#ifdef PROFILE
    const unsigned long long start = get_clock_gettime_ns();
#endif

    unsigned int address_align;
#ifdef USE_CUDA
    assert(sizeof(cl_mem) == 8);

    const int max_n_allocs = 1;
    address_align = 256;
    size_t max_alloc_size, global_mem_size;

    CHECK_DRIVER(cuCtxPushCurrent(ctx));

    CUdevice dev;
    CHECK_DRIVER(cuCtxGetDevice(&dev));
    const int device_index = dev;

    size_t mem_free, mem_total;
    CHECK_DRIVER(cuMemGetInfo(&mem_free, &mem_total));
    /*
     * There appear to be some hidden limits to page-locked host allocation on
     * some systems which mean we want to split into multiple allocations if we
     * have a lot of CUDA device memory. This limit ensures that.
     */
    const size_t limit_to = 11LU * 1024LU * 1024LU * 1024LU;
    max_alloc_size = mem_free;
    if (max_alloc_size > limit_to) max_alloc_size = limit_to;

    global_mem_size = mem_free;
#else
    cl_ulong global_mem_size, max_alloc_size;
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_GLOBAL_MEM_SIZE,
                sizeof(global_mem_size), &global_mem_size, NULL));
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_MEM_ALLOC_SIZE,
                sizeof(max_alloc_size), &max_alloc_size, NULL));
    if (limit_size != 0 && global_mem_size > limit_size) {
        global_mem_size = limit_size;
    }

    const int max_n_allocs = (global_mem_size + max_alloc_size - 1) / max_alloc_size;
    CHECK(clGetDeviceInfo(dev, CL_DEVICE_MEM_BASE_ADDR_ALIGN,
                sizeof(address_align), &address_align, NULL));
#endif

#ifdef VERBOSE
    fprintf(stderr, "Creating allocator for device %d given %llu bytes of "
            "global mem, max single allocation size of %llu, out of %d "
            "allocations\n", device_index, global_mem_size, max_alloc_size,
            max_n_allocs);
#endif

    cl_allocator *allocator = (cl_allocator *)malloc(sizeof(cl_allocator));
    CHECK_ALLOC(allocator);
    allocator->allocs = (cl_alloc *)malloc(max_n_allocs * sizeof(cl_alloc));
    CHECK_ALLOC(allocator->allocs);
    allocator->address_align = address_align;
#ifdef USE_CUDA
    allocator->cu_ctx = ctx;
#endif
    allocator->device_index = device_index;

    int i = 0;
    while (i < max_n_allocs) {
        size_t alloc_size = max_alloc_size;
        size_t leftover = global_mem_size - (i * max_alloc_size);
        if (leftover < alloc_size) {
            alloc_size = leftover;
        }

        cl_mem mem;
#ifdef USE_CUDA
        CUresult err;
#else
        int err;
#endif
        // Keep allocs at least 20MB
        bool success = false;
        while (alloc_size > 20 * 1024 * 1024) {
#ifdef USE_CUDA
            err = cuMemAlloc(&mem, alloc_size);
            if (err == CUDA_ERROR_OUT_OF_MEMORY) {
                alloc_size -= (20 * 1024 * 1024);
            } else {
                CHECK_DRIVER(err);
                success = true;
                break;
            }
#else
            mem = clCreateBuffer(ctx, alloc_flags, alloc_size, NULL,
                    &err);
            CHECK(err);
            err = clEnqueueWriteBuffer(cmd, mem, CL_TRUE, 0, sizeof(err), &err, 0, NULL, NULL);
            if (err == CL_MEM_OBJECT_ALLOCATION_FAILURE) {
                alloc_size -= (20 * 1024 * 1024);
                CHECK(clReleaseMemObject(mem));
            } else {
                CHECK(err);
                success = true;
                break;
            }
#endif
        }

        if (!success) break;

#ifdef VERBOSE
        fprintf(stderr, "Pre-allocated allocation %d with size %llu\n", i,
                alloc_size);
#endif

#ifdef USE_CUDA
        void *pinned;
        CHECK_DRIVER(cuMemAllocHost(&pinned, alloc_size));
#else
        /*
         * If you start seeing memory allocation errors from here, it is very
         * possible that the operating system is not unpinning pages when a
         * process exits suddenly.
         */
        // void *pinned = NULL;
        void *pinned = clEnqueueMapBuffer(cmd, mem, CL_TRUE, CL_MAP_WRITE, 0,
                alloc_size, 0, NULL, NULL, &err);
        CHECK(err);
#endif

        (allocator->allocs)[i].mem = mem;
        (allocator->allocs)[i].size = alloc_size;
        (allocator->allocs)[i].free_bytes = alloc_size;
        (allocator->allocs)[i].allocator = allocator;
#ifdef PROFILE_LOCKS
        (allocator->allocs)[i].contention = 0ULL;
#endif
        (allocator->allocs)[i].pinned = (char *)pinned;

        int perr = pthread_mutex_init(&(allocator->allocs)[i].lock, NULL);
        ASSERT(perr == 0);

        cl_region *first_region = (cl_region *)malloc(sizeof(cl_region));
        CHECK_ALLOC(first_region);
        first_region->sub_mem = (cl_mem)NULL;
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

        i++;
    }
    allocator->nallocs = i;
    EXIT_TRACE("init_allocator");

#ifdef USE_CUDA
    CUcontext old_ctx;
    CHECK_DRIVER(cuCtxPopCurrent(&old_ctx));
    assert(old_ctx == ctx);
#endif

#ifdef PROFILE
    __sync_fetch_and_add(&acc_init_time, get_clock_gettime_ns() - start);
#endif
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
        fprintf(stderr, "          free_bytes = %lu (expected=%lu)\n",
                free_bytes, alloc->free_bytes);
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

#ifdef PROFILE_LOCKS
unsigned long long get_contention(cl_allocator *allocator) {
    unsigned long long sum = 0ULL;
    for (int i = 0; i < allocator->nallocs; i++) {
        cl_alloc *curr = allocator->allocs + i;
        lock_alloc(curr);
        sum += curr->contention;
        unlock_alloc(curr);
    }
    return sum;
}
#else
unsigned long long get_contention(cl_allocator *allocator) {
    fprintf(stderr, "ERROR: get_contention : clalloc not compiled with "
            "contention information support (-DPROFILE_LOCKS)\n");
    exit(1);
}
#endif

#ifdef PROFILE
void print_clalloc_profile(int thread) {
    fprintf(stderr, "%d : clalloc profile: init - %llu ns | alloc - %llu ns | "
            "realloc - %llu ns | free - %llu ns\n", thread, acc_init_time,
            acc_alloc_time, acc_realloc_time, acc_free_time);
}
#else
void print_clalloc_profile(int thread) {
    fprintf(stderr, "ERROR: print_profile : clalloc not compiled with profile "
            "information (-DPROFILE)\n");
    exit(1);
}
#endif
