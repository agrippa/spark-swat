#include <jni.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "kernel_arg.h"
#include "bridge.h"
#include "common.h"
#include "ocl_util.h"

#ifdef VERBOSE
#define TRACE_MSG(...) fprintf(stderr, __VA_ARGS__)
#else
#define TRACE_MSG(...)
#endif

#ifdef __cplusplus
extern "C" {
#endif
static unsigned long long get_clock_gettime_ns();
#ifdef __cplusplus
}
#endif

#ifdef PROFILE_OPENCL
static const unsigned long long app_start_time = get_clock_gettime_ns();
#endif

static device_context *device_ctxs = NULL;
static int n_device_ctxs = 0;
/*
 * Only used when initializing device contexts to ensure only one thread
 * initializes them. Should be very little contention on this lock.
 */
static pthread_mutex_t device_ctxs_lock = PTHREAD_MUTEX_INITIALIZER;
#ifdef PROFILE_LOCKS
static unsigned long long device_ctxs_lock_contention = 0ULL;
#endif
static int *virtual_devices = NULL;
static int n_virtual_devices = 0;

/*
 * Cache a mapping from unique RDD identifier to the number of items loaded for
 * that part of the RDD.
 */
static pthread_rwlock_t nloaded_cache_lock = PTHREAD_RWLOCK_INITIALIZER;
static map<rdd_partition_offset, int> *nloaded_cache;
#ifdef PROFILE_LOCKS
static unsigned long long nloaded_cache_lock_contention = 0ULL;
#endif

/*
 * Cache certain JNI things.
 */
static jmethodID denseVectorValuesMethod;

static jmethodID sparseVectorValuesMethod;
static jmethodID sparseVectorIndicesMethod;

const unsigned zero = 0;

/*
 * Read lock is acquired when looking for a hint for which device to run on.
 * Read lock is also acquired when checking if there's a cached version of a
 * piece of an RDD. Write lock is acquired when we need to update metadata on
 * the RDD caching.
 */
#define RDD_CACHE_BUCKETS 256
static pthread_mutex_t rdd_cache_locks[RDD_CACHE_BUCKETS];
#ifdef PROFILE_LOCKS
static unsigned long long rdd_cache_contention[RDD_CACHE_BUCKETS];
#endif

/*
 * Inter-device RDD cache, bucketed by partition.
 */
static map<rdd_partition_offset, map<int, cl_region *> *> *rdd_caches[RDD_CACHE_BUCKETS];

#define ARG_MACRO(ltype, utype) \
JNI_JAVA(void, OpenCLBridge, set##utype##Arg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, j##ltype arg) { \
    ENTER_TRACE("set"#utype"Arg"); \
    swat_context *context = (swat_context *)lctx; \
    add_pending_##ltype##_arg(context, index, arg); \
    EXIT_TRACE("set"#utype"Arg"); \
}

#define SET_ARRAY_ARG_MACRO(ltype, utype, ctype) \
JNI_JAVA(jboolean, OpenCLBridge, set##utype##ArrayArgImpl) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, \
         j##ltype##Array arg, jint argLength, jlong broadcastId, jint rddid, \
         jint partitionid, jint offsetid, jint componentid, jlong nativeBuffer, \
         jboolean persistent) { \
    ENTER_TRACE("set"#utype"ArrayArg"); \
    void *buffer = (void *)nativeBuffer; \
    jsize len = argLength * sizeof(ctype); \
    device_context *dev_ctx = (device_context *)l_dev_ctx; \
    swat_context *context = (swat_context *)lctx; \
    jboolean isCopy; \
    if (broadcastId >= 0) { \
        ASSERT(persistent); \
        ASSERT_MSG(rddid < 0 && componentid >= 0, "broadcast check"); \
        broadcast_id uuid(broadcastId, componentid); \
        lock_bcast_cache(dev_ctx); \
        \
        cl_region *region = NULL; \
        map<broadcast_id, cl_region *>::iterator found = dev_ctx->broadcast_cache->find(uuid); \
        if (found != dev_ctx->broadcast_cache->end()) { \
            region = found->second; \
        } \
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index)); \
        if (reallocated) { \
            TRACE_MSG("caching broadcast %ld %d\n", broadcastId, componentid); \
            add_pending_region_arg(context, index, true, persistent, false, region); \
        } else { \
            void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
            CHECK_JNI(arr) \
            if (buffer) memcpy(buffer, arr, len); \
            cl_region *new_region = set_and_write_kernel_arg( \
                    buffer ? buffer : arr, len, index, context, dev_ctx, broadcastId, \
                    rddid, persistent, buffer == NULL); \
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
            if (new_region == NULL) { \
                unlock_bcast_cache(dev_ctx); \
                return false; \
            } \
            (*dev_ctx->broadcast_cache)[uuid] = new_region; \
            TRACE_MSG("adding broadcast %ld %d to cache\n", broadcastId, \
                    componentid); \
        } \
        unlock_bcast_cache(dev_ctx); \
    } else if (rddid >= 0) { \
        ASSERT(!persistent); \
        ASSERT_MSG(broadcastId < 0 && partitionid >= 0 && offsetid >= 0 && \
                componentid >= 0, "check RDD"); \
        rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid); \
        lock_rdd_cache(uuid); \
        cl_region *region = check_rdd_cache(uuid, dev_ctx); \
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index)); \
        if (reallocated) { \
            TRACE_MSG("caching rdd=%d partition=%d offset=%d component=%d\n", \
                    rddid, partitionid, offsetid, componentid); \
            add_pending_region_arg(context, index, true, persistent, false, region); \
        } else { \
            void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
            CHECK_JNI(arr); \
            if (buffer) memcpy(buffer, arr, len); \
            cl_region *new_region = set_and_write_kernel_arg( \
                    buffer ? buffer : arr, len, index, context, dev_ctx, broadcastId, \
                    rddid, persistent, buffer == NULL); \
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
            if (new_region == NULL) { \
                unlock_rdd_cache(uuid); \
                return false; \
            } \
            update_rdd_cache(uuid, new_region, dev_ctx->device_index); \
            TRACE_MSG("adding rdd=%d partition=%d offset=%d component=%d\n", \
                    rddid, partitionid, offsetid, componentid); \
        } \
        unlock_rdd_cache(uuid); \
    } else { \
        ASSERT_MSG(rddid < 0 && broadcastId < 0, "neither RDD or broadcast"); \
        void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
        CHECK_JNI(arr) \
        if (buffer) memcpy(buffer, arr, len); \
        cl_region *new_region = set_and_write_kernel_arg(buffer ? buffer : arr, \
                len, index, context, dev_ctx, broadcastId, rddid, persistent, \
                buffer == NULL); \
        jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
        if (new_region == NULL) return false; \
        \
    } \
    EXIT_TRACE("set"#utype"ArrayArg"); \
    return true; \
}

#define SET_PRIMITIVE_ARG_BY_NAME_MACRO(ltype, utype, desc) \
JNI_JAVA(void, OpenCLBridge, set##utype##ArgByName) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, jobject obj, \
         jstring name) { \
    ENTER_TRACE("set"#utype"ArgByName"); \
    swat_context *context = (swat_context *)lctx; \
    jclass enclosing_class = jenv->GetObjectClass(obj); \
    CHECK_JNI(enclosing_class) \
    const char *raw_name = jenv->GetStringUTFChars(name, NULL); \
    CHECK_JNI(raw_name) \
    jfieldID field = jenv->GetFieldID(enclosing_class, raw_name, desc); \
    CHECK_JNI(field) \
    jenv->ReleaseStringUTFChars(name, raw_name); \
    ltype val = jenv->Get##utype##Field(obj, field); \
    add_pending_##ltype##_arg(context, index, val); \
    EXIT_TRACE("set"#utype"ArgByName"); \
}

#ifdef __cplusplus
extern "C" {
#endif

#ifdef TRACE
void enter_trace(const char *lbl) {
    fprintf(stderr, "entering %s\n", lbl);
}

void exit_trace(const char *lbl) {
    fprintf(stderr, "leaving %s\n", lbl);
}
#endif

#if defined(PROFILE_LOCKS) || defined(PROFILE_OPENCL)
static unsigned long long get_clock_gettime_ns() {
    struct timespec t ={0,0};
    clock_gettime(CLOCK_MONOTONIC, &t);
    unsigned long long s = 1000000000ULL * (unsigned long long)t.tv_sec;
    return (unsigned long long)t.tv_nsec + s;
}
#endif

static inline void force_pthread_mutex_lock(pthread_mutex_t *mutex) {
    const int perr = pthread_mutex_lock(mutex);
    ASSERT(perr == 0);
}

static inline void force_pthread_mutex_unlock(pthread_mutex_t *mutex) {
    const int perr = pthread_mutex_unlock(mutex);
    ASSERT(perr == 0);
}

static void add_pending_arg(swat_context *context, int index, bool keep,
        bool dont_free, bool copy_out, enum arg_type type,
        region_or_scalar val) {
#ifdef VERBOSE
    fprintf(stderr, "add_pending_arg: thread=%d ctx=%p index=%d keep=%s "
            "dont_free=%s type=%d\n", context->host_thread_index, context, index,
            keep ? "true" : "false", dont_free ? "true" : "false", type);
#endif

    if (context->accumulated_arguments_len >= context->accumulated_arguments_capacity) {
        const int new_capacity = context->accumulated_arguments_capacity * 2;
        context->accumulated_arguments = (arg_value *)realloc(
                context->accumulated_arguments, new_capacity *
                sizeof(arg_value));
        CHECK_ALLOC(context->accumulated_arguments);
        context->accumulated_arguments_capacity = new_capacity;
    }

    ASSERT(context->accumulated_arguments_capacity >
            context->accumulated_arguments_len);
    const int acc_index = context->accumulated_arguments_len;
    (context->accumulated_arguments)[acc_index].index = index;
    (context->accumulated_arguments)[acc_index].keep = keep;
    (context->accumulated_arguments)[acc_index].dont_free = dont_free;
    (context->accumulated_arguments)[acc_index].copy_out = copy_out;
    (context->accumulated_arguments)[acc_index].type = type;
    (context->accumulated_arguments)[acc_index].val = val;
    context->accumulated_arguments_len = context->accumulated_arguments_len + 1;
}

static void add_pending_region_arg(swat_context *context, int index, bool keep,
        bool dont_free, bool copy_out, cl_region *region) {
    region_or_scalar val; val.region = region;
    add_pending_arg(context, index, keep, dont_free, copy_out, REGION, val);
}

static void add_pending_int_arg(swat_context *context, int index, int in) {
    region_or_scalar val; val.i = in;
    add_pending_arg(context, index, false, false, false, INT, val);
}

static void add_pending_float_arg(swat_context *context, int index, float in) {
    region_or_scalar val; val.f = in;
    add_pending_arg(context, index, false, false, false, FLOAT, val);
}

static void add_pending_double_arg(swat_context *context, int index, double in) {
    region_or_scalar val; val.d = in;
    add_pending_arg(context, index, false, false, false, DOUBLE, val);
}

/*
 * Utility functions that handle accessing data in the RDD cache. These utilities
 * assume the parent handles any necessary locking of rdd_cache_locks for us.
 */
static inline unsigned rdd_cache_bucket_for_partition(int partition) {
    return (partition % RDD_CACHE_BUCKETS);
}
static inline unsigned rdd_cache_bucket_for(rdd_partition_offset uuid) {
    return rdd_cache_bucket_for_partition(uuid.get_partition());
}

static inline cl_region *check_rdd_cache(rdd_partition_offset uuid,
        device_context *dev_ctx) {
    cl_region *region = NULL;
    map<rdd_partition_offset, map<int, cl_region *> *> *rdd_cache =
        rdd_caches[rdd_cache_bucket_for(uuid)];
    map<rdd_partition_offset, map<int, cl_region *> *>::iterator found =
        rdd_cache->find(uuid);
    if (found != rdd_cache->end()) {
        map<int, cl_region *> *cached = found->second;
        map<int, cl_region *>::iterator found_in_cache = cached->find(
                dev_ctx->device_index);
        if (found_in_cache != cached->end()) {
            region = found_in_cache->second;
        }
    }
    return region;
}

static inline void lock_rdd_cache_by_partition(int partition) {
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    const int err = pthread_mutex_lock(rdd_cache_locks +
            rdd_cache_bucket_for_partition(partition));
#ifdef PROFILE_LOCKS
    rdd_cache_contention[partition] += (get_clock_gettime_ns() - start);
#endif
    ASSERT(err == 0);
}

static inline void unlock_rdd_cache_by_partition(int partition) {
    const int err = pthread_mutex_unlock(rdd_cache_locks +
            rdd_cache_bucket_for_partition(partition));
    ASSERT(err == 0);
}

static inline void lock_rdd_cache(rdd_partition_offset uuid) {
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    const int err = pthread_mutex_lock(rdd_cache_locks +
            rdd_cache_bucket_for(uuid));
#ifdef PROFILE_LOCKS
    rdd_cache_contention[rdd_cache_bucket_for(uuid)] +=
        (get_clock_gettime_ns() - start);
#endif
    ASSERT(err == 0);
}

static inline void unlock_rdd_cache(rdd_partition_offset uuid) {
    const int err = pthread_mutex_unlock(rdd_cache_locks +
            rdd_cache_bucket_for(uuid));
    ASSERT(err == 0);
}

static void remove_from_rdd_cache_if_present(rdd_partition_offset uuid, int dev) {
    map<rdd_partition_offset, map<int, cl_region *> *> *rdd_cache =
        rdd_caches[rdd_cache_bucket_for(uuid)];
    map<rdd_partition_offset, map<int, cl_region *> *>::iterator found = rdd_cache->find(uuid);
    if (found != rdd_cache->end()) {
        map<int, cl_region *> *for_uuid = found->second;
        map<int, cl_region *>::iterator dev_found = for_uuid->find(dev);
        if (dev_found != for_uuid->end()) {
            for_uuid->erase(dev_found);
        }
    }
}

static void update_rdd_cache(rdd_partition_offset uuid, cl_region *new_region,
        int device_index) {
    const int rdd_bucket = rdd_cache_bucket_for(uuid);
    map<rdd_partition_offset, map<int, cl_region *> *> *rdd_cache = rdd_caches[rdd_bucket];
    if (rdd_cache->find(uuid) == rdd_cache->end()) {
        rdd_cache->insert(
                pair<rdd_partition_offset, map<int, cl_region *> *>(
                    uuid, new map<int, cl_region *>()));
    }
    (*rdd_cache->at(uuid))[device_index] = new_region;
}

/*
 * Utility functions that handle accessing data in the broadcast caches (one for
 * each device). These utilities assume the parent handles any necessary locking
 * of rdd_cache_locks for us.
 */
static inline void lock_bcast_cache(device_context *ctx) {
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    const int err = pthread_mutex_lock(&ctx->broadcast_lock);
#ifdef PROFILE_LOCKS
    ctx->broadcast_lock_contention += (get_clock_gettime_ns() - start);
#endif
    ASSERT(err == 0);
}

static inline void unlock_bcast_cache(device_context *ctx) {
    const int err = pthread_mutex_unlock(&ctx->broadcast_lock);
    ASSERT(err == 0);
}

static void remove_from_broadcast_cache_if_present(broadcast_id uuid,
        device_context *dev_ctx) {
    map<broadcast_id, cl_region *>::iterator found = dev_ctx->broadcast_cache->find(uuid);
    if (found != dev_ctx->broadcast_cache->end()) {
        dev_ctx->broadcast_cache->erase(found);
    }
}

static int checkExtension(char *exts, size_t ext_len, const char *ext) {
    unsigned start = 0;
    while (start < ext_len) {
        unsigned end = start;
        while (end < ext_len && exts[end] != ' ') {
            end++;
        }

        if (strncmp(exts + start, ext, end - start) == 0) {
            return 1;
        }

        start = end + 1;
    }
    return 0;
}

static int checkAllAssertions(cl_device_id device, int requiresDouble,
        int requiresHeap) {

    int result = 1;
    int requires_extension_check = (requiresDouble || requiresHeap);
    if (requires_extension_check) {
        size_t ext_len;
        CHECK(clGetDeviceInfo(device, CL_DEVICE_EXTENSIONS, 0, NULL, &ext_len));
        char *exts = (char *)malloc(ext_len);
        CHECK_ALLOC(exts);
        CHECK(clGetDeviceInfo(device, CL_DEVICE_EXTENSIONS, ext_len, exts, NULL));

        if (requiresDouble && !checkExtension(exts, ext_len, "cl_khr_fp64")) {
            result = 0;
        } else {
            if (requiresHeap &&
                    (!checkExtension(exts, ext_len, "cl_khr_global_int32_base_atomics") ||
                     !checkExtension(exts, ext_len, "cl_khr_global_int32_extended_atomics") ||
                     !checkExtension(exts, ext_len, "cl_khr_local_int32_base_atomics") ||
                     !checkExtension(exts, ext_len, "cl_khr_local_int32_extended_atomics"))) {
                result = 0;
            }
        }
        free(exts);
    }
    return result;
}

static void createHeapContext(heap_context *context, device_context *dev_ctx,
        size_t heap_size) {
    // The heap
    cl_region *heap = allocate_cl_region(heap_size, dev_ctx->allocator , NULL,
            NULL);
    ASSERT(heap);

    // free_index
    cl_region *free_index = allocate_cl_region(sizeof(zero),
            dev_ctx->allocator /* dev_ctx->heap_allocator */ , NULL, NULL);
    ASSERT(free_index);

    int *h_free_index = (int *)fetch_pinned(free_index);

    void *h_heap = fetch_pinned(heap);

#ifdef VERBOSE
    fprintf(stderr, "clalloc: allocating heap of size %lu (offset=%lu size=%lu, "
            "region=%p), free_index of size %lu (offset=%lu size=%lu "
            "region=%p)\n", heap_size, heap->offset, heap->size, heap,
            sizeof(zero), free_index->offset, free_index->size, free_index);
#endif

    context->dev_ctx = dev_ctx;
    context->heap = heap;
    context->free_index = free_index;
    context->id = dev_ctx->count_heaps;
    dev_ctx->count_heaps = dev_ctx->count_heaps + 1;
    context->pinned_h_free_index = h_free_index;
    context->pinned_h_heap = h_heap;

    context->heap_size = heap_size;

    // context->h_heap_in_use = 0;
    // int perr = pthread_mutex_init(&context->h_heap_lock, NULL);
    // ASSERT(perr == 0);
    // perr = pthread_cond_init(&context->h_heap_cond, NULL);
    // ASSERT(perr == 0);
}

static void populateDeviceContexts(JNIEnv *jenv, jint n_heaps_per_device,
        size_t heap_size, double perc_high_performance_buffers,
        bool createCpuContexts) {
    // Try to avoid having to do any locking
    if (device_ctxs != NULL) {
        return;
    }

    /*
     * clGetPlatformIDs called inside get_num_opencl_platforms is supposed to be
     * thread-safe... That doens't stop the Intel OpenCL library from
     * segfaulting or throwing an error if it is called from multiple threads at
     * once. If I had to guess, some Intel initialization is not thread-safe
     * even though the core code of clGetPlatformIDs is. Work around is just to
     * ensure no thread makes this call at the same time using a mutex, seems to
     * work fine but this shouldn't be happening to begin with.
     */
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int perr = pthread_mutex_lock(&device_ctxs_lock);
#ifdef PROFILE_LOCKS
    const unsigned long long elapsed = (get_clock_gettime_ns() - start);
    device_ctxs_lock_contention += elapsed;
#endif
    ASSERT(perr == 0);

    // Double check after locking
    if (device_ctxs == NULL) {
        cl_uint total_num_devices = get_total_num_devices();
        device_context *tmp_device_ctxs = (device_context *)malloc(
                total_num_devices * sizeof(device_context));
        CHECK_ALLOC(tmp_device_ctxs);
        n_device_ctxs = total_num_devices;

        cl_uint num_platforms = get_num_opencl_platforms();

        cl_platform_id *platforms =
            (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
        CHECK_ALLOC(platforms);
        CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));

        unsigned global_device_id = 0;
        for (unsigned platform_index = 0; platform_index < num_platforms; platform_index++) {
            cl_uint num_devices = get_num_devices(platforms[platform_index]);
            cl_device_id *devices = (cl_device_id *)malloc(num_devices * sizeof(cl_device_id));
            CHECK_ALLOC(devices);
            CHECK(clGetDeviceIDs(platforms[platform_index], CL_DEVICE_TYPE_ALL,
                        num_devices, devices, NULL));

            for (unsigned i = 0; i < num_devices; i++) {
                cl_device_id curr_dev = devices[i];
#ifdef VERBOSE
                char *device_name = get_device_name(curr_dev);
                fprintf(stderr, "SWAT %d: platform %d, device %d, %s (%s), %d bytes/ptr\n",
                        global_device_id, platform_index, i, device_name,
                        get_device_type_str(curr_dev),
                        get_device_pointer_size_in_bytes(curr_dev));
                free(device_name);
#endif

                if (get_device_type(curr_dev) == CL_DEVICE_TYPE_CPU && !createCpuContexts) {
                    // TODO remove this if we want to use CPU devices in the future
                    memset(tmp_device_ctxs + global_device_id, 0x00, sizeof(device_context));
                    tmp_device_ctxs[global_device_id].dev = curr_dev;
                    global_device_id++;
                    continue;
                }

                cl_int err;
                cl_context_properties ctx_props[] = { CL_CONTEXT_PLATFORM,
                    (cl_context_properties)platforms[platform_index], 0 };
                cl_context ctx = clCreateContext(ctx_props, 1, &curr_dev, NULL, NULL, &err);
                CHECK(err);

                cl_command_queue_properties props = 0;
#ifndef __APPLE__
                /*
                 * Need to keep this disabled. If we have N heaps (enabling N
                 * parallel kernels) and M output buffers where M < N, then
                 * out-of-order kernel execution can lead to later kernels
                 * acquiring all output buffers, and earlier kernel coming along
                 * and blocking trying to acquire an output buffer, while the
                 * output thread blocks and waits on the earlier kernel ->
                 * deadlock. In general, this makes it harder to reason about
                 * the execution order of OpenCL commands and makes it easier to
                 * get into bad situations.
                 */
                // props |= CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE;
#endif
#ifdef PROFILE_OPENCL
                props |= CL_QUEUE_PROFILING_ENABLE;
#endif

                cl_command_queue cmd = clCreateCommandQueue(ctx, curr_dev,
                        props, &err);
                CHECK(err);

                tmp_device_ctxs[global_device_id].platform = platforms[platform_index];
                tmp_device_ctxs[global_device_id].dev = curr_dev;
                tmp_device_ctxs[global_device_id].ctx = ctx;
                tmp_device_ctxs[global_device_id].cmd = cmd;
                tmp_device_ctxs[global_device_id].device_index = global_device_id;

                int perr = pthread_mutex_init(
                        &(tmp_device_ctxs[global_device_id].broadcast_lock),
                        NULL);
                ASSERT(perr == 0);
                perr = pthread_mutex_init(
                        &(tmp_device_ctxs[global_device_id].program_cache_lock),
                        NULL);
                ASSERT(perr == 0);
                perr = pthread_mutex_init(
                        &(tmp_device_ctxs[global_device_id].heap_cache_lock),
                        NULL);
                ASSERT(perr == 0);

#ifdef PROFILE_LOCKS
                tmp_device_ctxs[global_device_id].broadcast_lock_contention =
                    0ULL;
                tmp_device_ctxs[global_device_id].program_cache_lock_contention =
                    0ULL;
                tmp_device_ctxs[global_device_id].heap_cache_lock_contention =
                    0ULL;
                tmp_device_ctxs[global_device_id].heap_cache_blocked = 0ULL;
#endif

                tmp_device_ctxs[global_device_id].allocator = init_allocator(
                        curr_dev, global_device_id,
                        CL_MEM_READ_WRITE | CL_MEM_ALLOC_HOST_PTR, 0, ctx, cmd);

                tmp_device_ctxs[global_device_id].broadcast_cache =
                    new map<broadcast_id, cl_region *>();
                tmp_device_ctxs[global_device_id].program_cache =
                    new map<string, cl_program>();
                heap_context *heap_cache_head = NULL;
                heap_context *heap_cache_tail = NULL;

                tmp_device_ctxs[global_device_id].count_heaps = 0;
                for (int h = 0; h < n_heaps_per_device; h++) {
                    heap_context *heap_ctx = (heap_context *)malloc(
                            sizeof(heap_context));
                    CHECK_ALLOC(heap_ctx);
                    createHeapContext(heap_ctx,
                            tmp_device_ctxs + global_device_id, heap_size);

                    if (heap_cache_head == NULL) {
                        heap_cache_head = heap_ctx;
                    }
                    if (heap_cache_tail != NULL) {
                        heap_cache_tail->next = heap_ctx;
                    }
                    heap_cache_tail = heap_ctx;
                }
                heap_cache_tail->next = NULL;
                tmp_device_ctxs[global_device_id].heap_cache_head = heap_cache_head;
                tmp_device_ctxs[global_device_id].heap_cache_tail = heap_cache_tail;
                tmp_device_ctxs[global_device_id].n_heaps = n_heaps_per_device;
                tmp_device_ctxs[global_device_id].heap_size = heap_size;

                global_device_id++;

                switch (get_device_type(curr_dev)) {
                    case (CL_DEVICE_TYPE_CPU):
                        n_virtual_devices += 0;
                        break;
                    case (CL_DEVICE_TYPE_GPU):
                        n_virtual_devices += 1;
                        break;
                    default:
                        fprintf(stderr, "Unsupported device type %d\n",
                                (int)get_device_type(curr_dev));
                        exit(1);
                }
            }
            free(devices);
        }

        free(platforms);

        virtual_devices = (int *)malloc(sizeof(int) * n_virtual_devices);
        CHECK_ALLOC(virtual_devices);
        int curr_virtual_device = 0;
        for (unsigned i = 0; i < total_num_devices; i++) {
            cl_device_id curr_dev = tmp_device_ctxs[i].dev;

            int weight;
            switch (get_device_type(curr_dev)) {
                case (CL_DEVICE_TYPE_CPU):
                    weight = 0;
                    break;
                case (CL_DEVICE_TYPE_GPU):
                    weight = 1;
                    break;
                default:
                    fprintf(stderr, "Unsupported device type %d\n",
                            (int)get_device_type(curr_dev));
                    exit(1);
            }

            for (int j = 0; j < weight; j++) {
                virtual_devices[curr_virtual_device++] = i;
            }
        }
        ASSERT(curr_virtual_device == n_virtual_devices);

        // Get dense vector JNI methods
        jclass denseVectorClass = jenv->FindClass(
                "org/apache/spark/mllib/linalg/DenseVector");
        CHECK_JNI(denseVectorClass);
        denseVectorValuesMethod = jenv->GetMethodID(denseVectorClass, "values",
                "()[D");
        CHECK_JNI(denseVectorValuesMethod);

        // Get sparse vector JNI methods
        jclass sparseVectorClass = jenv->FindClass(
                "org/apache/spark/mllib/linalg/SparseVector");
        CHECK_JNI(sparseVectorClass);
        sparseVectorValuesMethod = jenv->GetMethodID(sparseVectorClass, "values",
                "()[D");
        CHECK_JNI(sparseVectorValuesMethod);
        sparseVectorIndicesMethod = jenv->GetMethodID(sparseVectorClass, "indices",
                "()[I");
        CHECK_JNI(sparseVectorIndicesMethod);

        device_ctxs = tmp_device_ctxs;

        // Initialize cache state
        for (int i = 0; i < RDD_CACHE_BUCKETS; i++) {
            rdd_cache_locks[i] = PTHREAD_MUTEX_INITIALIZER;
#ifdef PROFILE_LOCKS
            rdd_cache_contention[i] = 0ULL;
#endif
            rdd_caches[i] = new map<rdd_partition_offset, map<int, cl_region *> *>();
        }

        nloaded_cache = new map<rdd_partition_offset, int>();
    }

    perr = pthread_mutex_unlock(&device_ctxs_lock);
    ASSERT(perr == 0);
}

JNI_JAVA(jint, OpenCLBridge, getDeviceToUse)
        (JNIEnv *jenv, jclass clazz, jint hint, jint host_thread_index,
         jint n_heaps_per_device, jint heap_size,
         jdouble perc_high_performance_buffers, jboolean create_cpu_contexts) {
    ENTER_TRACE("getDeviceToUse");
    populateDeviceContexts(jenv, n_heaps_per_device, heap_size,
            perc_high_performance_buffers, create_cpu_contexts);

    int result;
    if (hint != -1) {
        result = hint;
    } else {
        result = virtual_devices[host_thread_index % n_virtual_devices];
    }
    EXIT_TRACE("getDeviceToUse");
    return result;
}

JNI_JAVA(jint, OpenCLBridge, getDeviceHintFor)
        (JNIEnv *jenv, jclass clazz, jint rdd, jint partition, jint offset,
         jint component) {
    ENTER_TRACE("getDeviceHintFor");
    ASSERT(rdd >= 0);
    rdd_partition_offset uuid(rdd, partition, offset, component);
    int result = -1;

    lock_rdd_cache(uuid);

    if (device_ctxs) { // if we've been initialized
        map<rdd_partition_offset, map<int, cl_region *> *> *rdd_cache =
            rdd_caches[rdd_cache_bucket_for(uuid)];
        map<rdd_partition_offset, map<int, cl_region *> *>::iterator found =
            rdd_cache->find(uuid);
        if (found != rdd_cache->end()) {
            for (map<int, cl_region *>::iterator i = found->second->begin(),
                    e = found->second->end(); i != e; i++) {
                cl_region *region = i->second;
                /*
                 * If this RDD might still be persisted on a device, try to map the
                 * computation to that device.
                 */
                if (!region->invalidated) {
                    ASSERT(GET_DEVICE_FOR(region) == i->first);
                    result = i->first;
                    break;
                }
            }
        }
    }

    unlock_rdd_cache(uuid);

    EXIT_TRACE("getDeviceHintFor");
    return result;
}

JNI_JAVA(jlong, OpenCLBridge, getActualDeviceContext)
        (JNIEnv *jenv, jclass clazz, jint device_index, jint n_heaps_per_device,
         jint heap_size, jdouble perc_high_performance_buffers,
         jboolean create_cpu_contexts) {

    populateDeviceContexts(jenv, n_heaps_per_device, heap_size,
            perc_high_performance_buffers, create_cpu_contexts);

    ASSERT(device_index < n_device_ctxs);
    return (jlong)(device_ctxs + device_index);
}

/*
 * Only called at the very end of execution so free all remaining arguments as
 * none will be accessible again.
 */
JNI_JAVA(void, OpenCLBridge, cleanupSwatContext)
        (JNIEnv *jenv, jclass clazz, jlong l_ctx, jlong l_dev_ctx) {
    ENTER_TRACE("cleanupSwatContext");
    swat_context *ctx = (swat_context *)l_ctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;

    cl_allocator *allocator = NULL;
    for (int index = 0; index < ctx->global_arguments_len; index++) {
        arg_value *curr = ctx->global_arguments + index;
        if (curr->type == REGION && curr->val.region) {
            cl_region *region = curr->val.region;

            if (allocator) {
                ASSERT(allocator == region->grandparent->allocator);
            } else {
                allocator = region->grandparent->allocator;
            }

#ifdef VERBOSE
            fprintf(stderr, "clalloc: cleanupSwatContext freeing region=%p "
                    "keep=%s offset=%lu size=%lu\n", region,
                    curr->keep ? "true" : "false", region->offset, region->size);
#endif
            free_cl_region(region, curr->keep);
        }
    }
#ifdef VERBOSE
#ifdef VERY_VERBOSE
    fprintf(stderr, "After cleanupSwatContext:\n");
    print_allocator(dev_ctx->allocator, ctx->host_thread_index);
#endif
#endif

    ctx->freed_native_input_buffers = NULL;
    ctx->global_arguments_len = 0;

#ifdef PROFILE_CLALLOC
    print_clalloc_profile();
#endif

#ifdef PROFILE_LOCKS
    // Global
    unsigned long long local_device_ctxs_lock_contention;
    unsigned long long total_rdd_cache_contention = 0ULL;
    unsigned long long local_rdd_cache_contention[RDD_CACHE_BUCKETS];
    unsigned long long local_nloaded_cache_lock_contention;
    // SWAT context
    unsigned long long local_kernel_lock_contention;
    unsigned long long local_freed_native_input_buffers_lock_contention;
    unsigned long long local_freed_native_input_buffers_blocked;
    unsigned long long local_completed_kernels_lock_contention;
    unsigned long long local_completed_kernels_blocked;
    // unsigned long long local_out_buffers_lock_contention;
    // unsigned long long local_out_buffers_blocked;
    // Device context
    unsigned long long local_broadcast_lock_contention;
    unsigned long long local_program_cache_lock_contention;
    unsigned long long local_heap_cache_lock_contention;
    unsigned long long local_heap_cache_blocked;
    unsigned long long local_allocator_contention;

    force_pthread_mutex_lock(&device_ctxs_lock);
    local_device_ctxs_lock_contention = device_ctxs_lock_contention;
    force_pthread_mutex_unlock(&device_ctxs_lock);

    for (int i = 0; i < RDD_CACHE_BUCKETS; i++) {
        force_pthread_mutex_lock(rdd_cache_locks + i);
        local_rdd_cache_contention[i] = rdd_cache_contention[i];
        force_pthread_mutex_unlock(rdd_cache_locks + i);
        total_rdd_cache_contention += local_rdd_cache_contention[i];
    }

    force_pthread_mutex_lock(&ctx->kernel_lock);
    local_kernel_lock_contention = ctx->kernel_lock_contention;
    force_pthread_mutex_unlock(&ctx->kernel_lock);

    force_pthread_mutex_lock(&ctx->freed_native_input_buffers_lock);
    local_freed_native_input_buffers_lock_contention =
        ctx->freed_native_input_buffers_lock_contention;
    local_freed_native_input_buffers_blocked =
        ctx->freed_native_input_buffers_blocked;
    force_pthread_mutex_unlock(&ctx->freed_native_input_buffers_lock);

    force_pthread_mutex_lock(&ctx->completed_kernels_lock);
    local_completed_kernels_lock_contention =
        ctx->completed_kernels_lock_contention;
    local_completed_kernels_blocked = ctx->completed_kernels_blocked;
    force_pthread_mutex_unlock(&ctx->completed_kernels_lock);

    // force_pthread_mutex_lock(&ctx->out_buffers_lock);
    // local_out_buffers_lock_contention = ctx->out_buffers_lock_contention;
    // local_out_buffers_blocked = ctx->out_buffers_blocked;
    // force_pthread_mutex_unlock(&ctx->out_buffers_lock);

    force_pthread_mutex_lock(&dev_ctx->broadcast_lock);
    local_broadcast_lock_contention = dev_ctx->broadcast_lock_contention;
    force_pthread_mutex_unlock(&dev_ctx->broadcast_lock);

    force_pthread_mutex_lock(&dev_ctx->program_cache_lock);
    local_program_cache_lock_contention = dev_ctx->program_cache_lock_contention;
    force_pthread_mutex_unlock(&dev_ctx->program_cache_lock);

    force_pthread_mutex_lock(&dev_ctx->heap_cache_lock);
    local_heap_cache_lock_contention = dev_ctx->heap_cache_lock_contention;
    local_heap_cache_blocked = dev_ctx->heap_cache_blocked;
    force_pthread_mutex_unlock(&dev_ctx->heap_cache_lock);

    local_nloaded_cache_lock_contention =
        (volatile unsigned long long)nloaded_cache_lock_contention;

    local_allocator_contention = get_contention(dev_ctx->allocator);

    fprintf(stderr, "LOCK SUMMARY, device=%d, host thread=%d\n",
            dev_ctx->device_index, ctx->host_thread_index);
    fprintf(stderr, "  device_ctxs_lock_contention                = %llu\n",
            local_device_ctxs_lock_contention);
    fprintf(stderr, "  rdd_cache_lock_contention                  = %llu\n",
            total_rdd_cache_contention);
    fprintf(stderr, "  kernel_lock_contention                     = %llu\n",
            local_kernel_lock_contention);
    fprintf(stderr, "  freed_native_input_buffers_lock_contention = %llu\n",
            local_freed_native_input_buffers_lock_contention);
    fprintf(stderr, "  freed_native_input_buffers_blocked         = %llu\n",
            local_freed_native_input_buffers_blocked);
    fprintf(stderr, "  completed_kernels_lock_contention          = %llu\n",
            local_completed_kernels_lock_contention);
    fprintf(stderr, "  completed_kernels_blocked                  = %llu\n",
            local_completed_kernels_blocked);
    fprintf(stderr, "  broadcast_lock_contention                  = %llu\n",
            local_broadcast_lock_contention);
    fprintf(stderr, "  program_cache_lock_contention              = %llu\n",
            local_program_cache_lock_contention);
    fprintf(stderr, "  heap_cache_lock_contention                 = %llu\n",
            local_heap_cache_lock_contention);
    fprintf(stderr, "  heap_cache_blocked                         = %llu\n",
            local_heap_cache_blocked);
    fprintf(stderr, "  nloaded_cache_lock_contention              = %llu\n",
            local_nloaded_cache_lock_contention);
    fprintf(stderr, "  allocator_contention                       = %llu\n",
            local_allocator_contention);

#endif
    EXIT_TRACE("cleanupSwatContext");
}

JNI_JAVA(void, OpenCLBridge, resetSwatContext)(JNIEnv *jenv, jclass clazz,
        jlong lctx) {
    swat_context *ctx = (swat_context *)lctx;
    fprintf(stderr, "thread = %d ctx = %ld global_arguments_len=%d\n", ctx->host_thread_index, lctx, ctx->global_arguments_len);

    ASSERT(ctx->accumulated_arguments_len == 0);
    ASSERT(ctx->accumulated_arguments_capacity > 0);
    ASSERT(ctx->global_arguments_len == 0);
    ASSERT(ctx->global_arguments_capacity > 0);

    ctx->last_write_event = NULL;
#ifdef PROFILE_OPENCL
    ASSERT(ctx->acc_write_events_length == 0);
#endif

    ctx->freed_native_input_buffers = NULL;

    ctx->run_seq_no = 0;

    ctx->completed_kernels = NULL;
}

JNI_JAVA(jlong, OpenCLBridge, createSwatContext)
        (JNIEnv *jenv, jclass clazz, jstring label, jstring source,
         jlong l_dev_ctx, jint host_thread_index, jboolean requiresDouble,
         jboolean requiresHeap, jint max_n_buffered, jintArray outArgSizes,
         jint outArgStart, jint nOutArgs) {
    ENTER_TRACE("createContext");
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_device_id device = dev_ctx->dev;

    ASSERT(checkAllAssertions(device, requiresDouble, requiresHeap) == 1);

#ifdef VERBOSE
    char *device_name = get_device_name(device);

    fprintf(stderr, "SWAT: host thread %d using device %s (index=%d), require "
            "double? %d, require heap? %d\n", host_thread_index, device_name,
            dev_ctx->device_index, requiresDouble, requiresHeap);
    free(device_name);
#endif
    const char *raw_label = jenv->GetStringUTFChars(label, NULL);
    CHECK_JNI(raw_label)
    std::string label_str(raw_label);
    jenv->ReleaseStringUTFChars(label, raw_label);

#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int perr = pthread_mutex_lock(&dev_ctx->program_cache_lock);
#ifdef PROFILE_LOCKS
    dev_ctx->program_cache_lock_contention += (get_clock_gettime_ns() - start);
#endif
    ASSERT(perr == 0);

#ifdef BRIDGE_DEBUG
    char *store_source;
#endif
    jsize source_len;

    cl_int err;
    cl_program program;
    if (dev_ctx->program_cache->find(label_str) != dev_ctx->program_cache->end()) {
        program = dev_ctx->program_cache->at(label_str);

#ifdef BRIDGE_DEBUG
        const char *raw_source = jenv->GetStringUTFChars(source, NULL);
        CHECK_JNI(raw_source)
        source_len = jenv->GetStringLength(source);
        store_source = (char *)malloc(source_len + 1);
        CHECK_ALLOC(store_source);
        memcpy(store_source, raw_source, source_len);
        store_source[source_len] = '\0';
        jenv->ReleaseStringUTFChars(source, raw_source);
#endif

    } else {
        const char *raw_source = jenv->GetStringUTFChars(source, NULL);
        CHECK_JNI(raw_source)
        source_len = jenv->GetStringLength(source);
#ifdef BRIDGE_DEBUG
        store_source = (char *)malloc(source_len + 1);
        CHECK_ALLOC(store_source);
        memcpy(store_source, raw_source, source_len);
        store_source[source_len] = '\0';
#endif

        size_t source_size[] = { (size_t)source_len };
        program = clCreateProgramWithSource(dev_ctx->ctx, 1, (const char **)&raw_source,
                source_size, &err);
        CHECK(err);

        err = clBuildProgram(program, 1, &device, NULL, NULL, NULL);
        if (err == CL_BUILD_PROGRAM_FAILURE) {
            size_t build_log_size;
            CHECK(clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG, 0,
                        NULL, &build_log_size));
            char *build_log = (char *)malloc(build_log_size + 1);
            CHECK_ALLOC(build_log);
            CHECK(clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG,
                        build_log_size, build_log, NULL));
            build_log[build_log_size] = '\0';
            fprintf(stderr, "%s\n\n", raw_source);
            fprintf(stderr, "Build failure:\n%s\n", build_log);
            free(build_log);
        }
        CHECK(err);
        jenv->ReleaseStringUTFChars(source, raw_source);

        bool success = dev_ctx->program_cache->insert(pair<string, cl_program>(label_str,
                    program)).second;
        ASSERT(success);
    }

    perr = pthread_mutex_unlock(&dev_ctx->program_cache_lock);
    ASSERT(perr == 0);

    cl_kernel kernel = clCreateKernel(program, "run", &err);
    CHECK(err);

    swat_context *context = (swat_context *)malloc(sizeof(swat_context));
    CHECK_ALLOC(context);
    context->kernel = kernel;
    perr = pthread_mutex_init(&context->kernel_lock, NULL);
    ASSERT(perr == 0);

#ifdef PROFILE_LOCKS
    context->kernel_lock_contention = 0ULL;
    context->freed_native_input_buffers_lock_contention = 0ULL;
    context->freed_native_input_buffers_blocked = 0ULL;
    context->completed_kernels_lock_contention = 0ULL;
    context->completed_kernels_blocked = 0ULL;
    // context->out_buffers_lock_contention = 0ULL;
    // context->out_buffers_blocked = 0ULL;
#endif

#ifdef PROFILE_OPENCL
    context->acc_write_events_capacity = 20;
    context->acc_write_events_length = 0;
    context->acc_write_events = (event_info *)malloc(
            context->acc_write_events_capacity * sizeof(event_info));
    CHECK_ALLOC(context->acc_write_events);
#endif

    context->host_thread_index = host_thread_index;

    context->accumulated_arguments_capacity = 20;
    context->accumulated_arguments = (arg_value *)malloc(
            context->accumulated_arguments_capacity * sizeof(arg_value));
    CHECK_ALLOC(context->accumulated_arguments);
    context->accumulated_arguments_len = 0;

    context->global_arguments_capacity = 20;
    context->global_arguments_len = 0;
    context->global_arguments = (arg_value *)malloc(
            context->global_arguments_capacity * sizeof(arg_value));
    CHECK_ALLOC(context->global_arguments);

    context->zeros = malloc(max_n_buffered * sizeof(int));
    CHECK_ALLOC(context->zeros);
    memset(context->zeros, 0x00, max_n_buffered * sizeof(int));
    context->zeros_capacity = max_n_buffered;

    context->freed_native_input_buffers = NULL;
    perr = pthread_mutex_init(&context->freed_native_input_buffers_lock, NULL);
    ASSERT(perr == 0);
    perr = pthread_cond_init(&context->freed_native_input_buffers_cond, NULL);
    ASSERT(perr == 0);

    context->completed_kernels = NULL;
    perr = pthread_mutex_init(&context->completed_kernels_lock, NULL);
    ASSERT(perr == 0);
    perr = pthread_cond_init(&context->completed_kernels_cond, NULL);
    ASSERT(perr == 0);

    context->last_write_event = NULL;

    context->run_seq_no = 0;
#ifdef BRIDGE_DEBUG
    context->debug_arguments = new map<int, kernel_arg *>();
    context->kernel_src = store_source;
    context->kernel_src_len = source_len;
    context->dump_index = 0;
#endif
    EXIT_TRACE("createContext");
    return (jlong)context;
}

static cl_region *get_mem(swat_context *context, device_context *dev_ctx,
        int index, size_t size, jlong broadcastId, jint rdd, jboolean persistent) {

    cl_region *region = allocate_cl_region(size, dev_ctx->allocator, NULL, NULL);
    if (region == NULL) return NULL;

#ifdef VERBOSE
    fprintf(stderr, "clalloc: thread=%d allocating region of size %lu bytes "
            "(offset=%lu size=%lu) for index=%d, region=%p\n",
            context->host_thread_index, size, region->offset, region->size,
            index, region);
#endif

#ifdef VERBOSE
    fprintf(stderr, "%d: Got %p for %lu bytes for index=%d\n",
            context->host_thread_index, region, size, index);
#endif

    add_pending_region_arg(context, index, broadcastId >= 0 || rdd >= 0,
            persistent, false, region);
    return region;
}

#ifdef PROFILE_OPENCL
static void add_event_to_list(event_info **list, cl_event event,
        const char *label, int *list_length, int *list_capacity) {
    if (*list_capacity == *list_length) {
        const int new_capacity = *list_capacity * 2;
        *list = (event_info *)realloc(*list, new_capacity * sizeof(event_info));
        CHECK_ALLOC(*list);
        *list_capacity = new_capacity;
    }

    const int label_length = strlen(label);

    (*list)[*list_length].event = event;
    char *lbl_copy = (char *)malloc(label_length + 1);
    CHECK_ALLOC(lbl_copy);
    memcpy(lbl_copy, label, label_length + 1);
    (*list)[*list_length].label = lbl_copy;
    (*list)[*list_length].timestamp = get_clock_gettime_ns();

    *list_length = *list_length + 1;
}
#endif

static cl_region *set_and_write_kernel_arg(void *host, size_t len, int index,
        swat_context *context, device_context *dev_ctx, jlong broadcastId,
        jint rdd, bool persistent, bool blocking) {
    cl_region *region = get_mem(context, dev_ctx, index, len,
            broadcastId, rdd, persistent);
    if (region == NULL) return NULL;

    if (blocking) {
        CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem,
                    CL_TRUE, 0, len, host, 0, NULL, NULL));
    } else {
        cl_event event;
        CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem,
                    CL_FALSE, 0, len, host, context->last_write_event ? 1 : 0,
                    context->last_write_event ? &context->last_write_event : NULL, &event));
#ifdef PROFILE_OPENCL
        add_event_to_list(&context->acc_write_events, event, "init_write",
                &context->acc_write_events_length,
                &context->acc_write_events_capacity);
#endif
        context->last_write_event = event;
    }

#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a memory buffer of size %lu\n", index,
            len);
#endif
    return region;
}

ARG_MACRO(int, Int)

SET_PRIMITIVE_ARG_BY_NAME_MACRO(int, Int, "I")
SET_PRIMITIVE_ARG_BY_NAME_MACRO(double, Double, "D")
SET_PRIMITIVE_ARG_BY_NAME_MACRO(float, Float, "F")

SET_ARRAY_ARG_MACRO(int, Int, int)
SET_ARRAY_ARG_MACRO(double, Double, double)
SET_ARRAY_ARG_MACRO(float, Float, float)
SET_ARRAY_ARG_MACRO(byte, Byte, jbyte)

JNI_JAVA(int, OpenCLBridge, getDevicePointerSizeInBytes)
        (JNIEnv *jenv, jclass clazz, jlong l_dev_ctx) {
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    return get_device_pointer_size_in_bytes(dev_ctx->dev);
}

static cl_region *is_cached(jlong broadcastId, jint rddid, jint partitionid,
        jint offsetid, jint componentid, device_context *dev_ctx,
        swat_context *context) {
    cl_region *region = NULL;

    if (broadcastId >= 0) {
        ASSERT(rddid < 0 && componentid >= 0);
        broadcast_id uuid(broadcastId, componentid);
        lock_bcast_cache(dev_ctx);

        map<broadcast_id, cl_region *>::iterator found = dev_ctx->broadcast_cache->find(uuid);
        if (found != dev_ctx->broadcast_cache->end()) {
            region = found->second;
        }
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index));
        unlock_bcast_cache(dev_ctx);

        if (!reallocated) region = NULL;
    } else if (rddid >= 0) {
        ASSERT(broadcastId < 0 && partitionid >= 0 && offsetid >= 0 && componentid >= 0); \
        rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid);

        lock_rdd_cache(uuid);
        region = check_rdd_cache(uuid, dev_ctx);
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index));
        unlock_rdd_cache(uuid);

        if (!reallocated) region = NULL;
    }

    return region;
}

JNI_JAVA(jint, OpenCLBridge, getMaxOffsetOfStridedVectors)(
        JNIEnv *jenv, jclass clazz, jint nvectors, jlong sizesBuffer,
        jlong offsetsBuffer, jint tiling) {
    ENTER_TRACE("getMaxOffsetOfStridedVectors");
    int *sizes = (int *)sizesBuffer;
    int *offsets = (int *)offsetsBuffer;

    // Look at the last tile of vectors
    int start_search;
    if (nvectors % tiling == 0) {
        start_search = nvectors - tiling;
    } else {
        start_search = nvectors - (nvectors % tiling);
    }
    int end_search = nvectors;

    int found_max = -1;
    for (int i = start_search; i < end_search; i++) {
        const int max_offset = offsets[i] + (sizes[i] - 1) * tiling;
        if (found_max == -1 || max_offset > found_max) {
            found_max = max_offset;
        }
    }
    ASSERT(found_max != -1);

    EXIT_TRACE("getMaxOffsetOfStridedVectors");
    return found_max;
}

JNI_JAVA(void, OpenCLBridge, transferOverflowSparseVectorBuffers)(JNIEnv *jenv,
        jclass clazz, jlong dstValuesBuffer, jlong dstIndicesBuffer,
        jlong dstSizesBuffer, jlong dstOffsetsBuffer, jlong srcValuesBuffer,
        jlong srcIndicesBuffer, jlong srcSizesBuffer, jlong srcOffsetsBuffer,
        jint vectorsUsed, jint elementsUsed, jint leftoverVectors,
        jint leftoverElements) {
    ENTER_TRACE("transferOverflowSparseVectorBuffers");

    double *dstValues = (double *)dstValuesBuffer;
    int *dstIndices = (int *)dstIndicesBuffer;
    int *dstSizes = (int *)dstSizesBuffer;
    int *dstOffsets = (int *)dstSizesBuffer;

    double *srcValues = (double *)srcValuesBuffer;
    int *srcIndices = (int *)srcIndicesBuffer;
    int *srcSizes = (int *)srcSizesBuffer;
    int *srcOffsets = (int *)srcSizesBuffer;

    memcpy(dstSizes, srcSizes + vectorsUsed, leftoverVectors * sizeof(int));
    memcpy(dstValues, dstValues + elementsUsed, leftoverElements * sizeof(double));
    memcpy(dstIndices, srcIndices + elementsUsed, leftoverElements * sizeof(int));
    for (int i = 0; i < leftoverVectors; i++) {
        dstOffsets[i] = srcOffsets[vectorsUsed + i] - elementsUsed;
    }

    EXIT_TRACE("transferOverflowSparseVectorBuffers");
}


JNI_JAVA(void, OpenCLBridge, transferOverflowDenseVectorBuffers)(JNIEnv *jenv,
        jclass clazz, jlong dstValuesBuffer, jlong dstSizesBuffer,
        jlong dstOffsetsBuffer, jlong srcValuesBuffer, jlong srcSizesBuffer,
        jlong srcOffsetsBuffer, jint vectorsUsed, jint elementsUsed,
        jint leftoverVectors, jint leftoverElements) {
    ENTER_TRACE("transferOverflowDenseVectorBuffers");

    double *dstValues = (double *)dstValuesBuffer;
    int *dstSizes = (int *)dstSizesBuffer;
    int *dstOffsets = (int *)dstSizesBuffer;

    double *srcValues = (double *)srcValuesBuffer;
    int *srcSizes = (int *)srcSizesBuffer;
    int *srcOffsets = (int *)srcSizesBuffer;

    memcpy(dstSizes, srcSizes + vectorsUsed, leftoverVectors * sizeof(int));
    memcpy(dstValues, dstValues + elementsUsed, leftoverElements * sizeof(double));
    for (int i = 0; i < leftoverVectors; i++) {
        dstOffsets[i] = srcOffsets[vectorsUsed + i] - elementsUsed;
    }

    EXIT_TRACE("transferOverflowDenseVectorBuffers");
}

JNI_JAVA(void, OpenCLBridge, deserializeStridedValuesFromNativeArray)(
        JNIEnv *jenv, jclass clazz, jobjectArray bufferTo, jint nToBuffer,
        jlong valuesBuffer, jlong sizesBuffer, jlong offsetsBuffer, jint index,
        jint tiling) {
    ENTER_TRACE("deserializeStridedValuesFromNativeArray");
    const int bufferToLength = tiling;

    double *values = (double *)valuesBuffer;
    int *sizes = (int *)sizesBuffer;
    int *offsets = (int *)offsetsBuffer;

    for (int i = 0; i < nToBuffer; i++) {
        const int size = sizes[index + i];
        const int offset = offsets[index + i];
        jdoubleArray jvmArray = jenv->NewDoubleArray(size);
        CHECK_JNI(jvmArray);

        double *arr = (double *)jenv->GetPrimitiveArrayCritical(jvmArray, NULL);
        CHECK_JNI(arr);
        for (int j = 0; j < size; j++) {
            arr[j] = values[offset + (j * tiling)];
        }
        jenv->ReleasePrimitiveArrayCritical(jvmArray, arr, 0);

        jenv->SetObjectArrayElement(bufferTo, i, jvmArray);
    }
    EXIT_TRACE("deserializeStridedValuesFromNativeArray");
}

JNI_JAVA(void, OpenCLBridge, deserializeStridedIndicesFromNativeArray)(
        JNIEnv *jenv, jclass clazz, jobjectArray bufferTo, jint nToBuffer,
        jlong indicesBuffer, jlong sizesBuffer, jlong offsetsBuffer, jint index,
        jint tiling) {
    ENTER_TRACE("deserializeStridedIndicesFromNativeArray");
    const int bufferToLength = tiling;

    int *indices = (int *)indicesBuffer;
    int *sizes = (int *)sizesBuffer;
    int *offsets = (int *)offsetsBuffer;

    for (int i = 0; i < nToBuffer; i++) {
        const int size = sizes[index + i];
        const int offset = offsets[index + i];
        jintArray jvmArray = jenv->NewIntArray(size);
        CHECK_JNI(jvmArray);

        int *arr = (int *)jenv->GetPrimitiveArrayCritical(jvmArray, NULL);
        CHECK_JNI(arr);
        for (int j = 0; j < size; j++) {
            arr[j] = indices[offset + (j * tiling)];
        }
        jenv->ReleasePrimitiveArrayCritical(jvmArray, arr, 0);

        jenv->SetObjectArrayElement(bufferTo, i, jvmArray);
    }
    EXIT_TRACE("deserializeStridedIndicesFromNativeArray");
}

JNI_JAVA(jobject, OpenCLBridge, getVectorValuesFromOutputBuffers)(
        JNIEnv *jenv, jclass clazz, jlongArray heapBuffers, jlong infoBuffer,
        jint slot, jint structSize, jint offsetOffset, jint offsetSize, jint sizeOffset,
        jint iterOffset, jboolean isIndices) {
    ENTER_TRACE("getDenseVectorValuesFromOutputBuffers");
    const int slotOffset = slot * structSize;
    char *info = ((char *)infoBuffer) + slotOffset;

#ifdef VERBOSE
    fprintf(stderr, "getVectorValuesFromOutputBuffers: slot=%d structSize=%d "
            "offsetOffset=%d offsetSize=%d sizeOffset=%d iterOffset=%d "
            "isIndices=%s\n", slot, structSize, offsetOffset, offsetSize,
            sizeOffset, iterOffset, isIndices ? "true" : "false");
#endif

    int offset;
    if (offsetSize == 4) {
        offset = *((int *)(info + offsetOffset));
    } else { // offsetSize == 8
        offset = *((long *)(info + offsetOffset));
    }
    const int size = *((int *)(info + sizeOffset));
    const int iter = *((int *)(info + iterOffset));

#ifdef VERBOSE
    fprintf(stderr, "getVectorValuesFromOutputBuffers: offset=%d size=%d "
            "iter=%d\n", offset, size, iter);
#endif

    long *buffers = (long *)jenv->GetPrimitiveArrayCritical(heapBuffers, NULL);
    ASSERT(buffers);
    char *heapBuffer = (char *)buffers[iter];
    jenv->ReleasePrimitiveArrayCritical(heapBuffers, buffers, JNI_ABORT);

    void *valuesInHeap = (void *)(heapBuffer + offset);

    jobject resultArray;
    if (isIndices) {
        jintArray jvmArray = jenv->NewIntArray(size);
        CHECK_JNI(jvmArray);

        int *arr = (int *)jenv->GetPrimitiveArrayCritical(jvmArray, NULL);
        CHECK_JNI(arr);
        memcpy(arr, valuesInHeap, size * sizeof(int));
        jenv->ReleasePrimitiveArrayCritical(jvmArray, arr, 0);
        resultArray = jvmArray;
    } else {
        jdoubleArray jvmArray = jenv->NewDoubleArray(size);
        CHECK_JNI(jvmArray);

        double *arr = (double *)jenv->GetPrimitiveArrayCritical(jvmArray, NULL);
        CHECK_JNI(arr);
        memcpy(arr, valuesInHeap, size * sizeof(double));
        jenv->ReleasePrimitiveArrayCritical(jvmArray, arr, 0);
        resultArray = jvmArray;
    }
    EXIT_TRACE("getDenseVectorValuesFromOutputBuffers");
    return resultArray;
}

JNI_JAVA(jdoubleArray, OpenCLBridge, deserializeChunkedValuesFromNativeArray)(
        JNIEnv *jenv, jclass clazz, jlong buffer, jlong infoBuffer,
        jint offsetOffset, jint sizeOffset, jint offsetSize) {
    ENTER_TRACE("deserializeChunkedValuesFromNativeArray");

    char *info = (char *)infoBuffer;
    int offset, size;
    if (offsetSize == 4) {
        offset = *((int *)(info + offsetOffset));
    } else { // offsetSize == 8
        offset = *((long *)(info + offsetOffset));
    }
    size = *((int *)(info + sizeOffset));

    double *values = (double *)(((char *)buffer) + offset);

    jdoubleArray jvmArray = jenv->NewDoubleArray(size);
    CHECK_JNI(jvmArray);

    double *arr = (double *)jenv->GetPrimitiveArrayCritical(jvmArray, NULL);
    CHECK_JNI(arr);
    memcpy(arr, values, size * sizeof(double));
    jenv->ReleasePrimitiveArrayCritical(jvmArray, arr, 0);

    EXIT_TRACE("deserializeChunkedValuesFromNativeArray");
    return jvmArray;
}

JNI_JAVA(jintArray, OpenCLBridge, deserializeChunkedIndicesFromNativeArray)(
        JNIEnv *jenv, jclass clazz, jlong buffer, jlong infoBuffer,
        jint offsetOffset, jint sizeOffset, jint offsetSize) {
    ENTER_TRACE("deserializeChunkedIndicesFromNativeArray");

    char *info = (char *)infoBuffer;
    int offset, size;
    if (offsetSize == 4) {
        offset = *((int *)(info + offsetOffset));
    } else { // offsetSize == 8
        offset = *((long *)(info + offsetOffset));
    }
    size = *((int *)(info + sizeOffset));

    int *indices = (int *)(((char *)buffer) + offset);

    jintArray jvmArray = jenv->NewIntArray(size);
    CHECK_JNI(jvmArray);

    int *arr = (int *)jenv->GetPrimitiveArrayCritical(jvmArray, NULL);
    CHECK_JNI(arr);
    memcpy(arr, indices, size * sizeof(int));
    jenv->ReleasePrimitiveArrayCritical(jvmArray, arr, 0);

    EXIT_TRACE("deserializeChunkedIndicesFromNativeArray");
    return jvmArray;
}

JNI_JAVA(jboolean, OpenCLBridge, setNativeArrayArgImpl)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index,
         jlong buffer, jint len, jlong broadcastId, jint rddid,
         jint partitionid, jint offsetid, jint componentid, jboolean persistent,
         jboolean blocking) {
    ENTER_TRACE("setNativeArrayArg");
#ifdef VERBOSE
    fprintf(stderr, "setNativeArrayArgImpl: index=%d buffer=%ld len=%d "
            "broadcastId=%ld rddid=%d partitionid=%d offsetid=%d componentid=%d "
            "persistent=%s blocking=%s\n", index, buffer, len, broadcastId,
            rddid, partitionid, offsetid, componentid,
            persistent ? "true" : "false", blocking ? "true" : "false");
#endif

    device_context *dev_ctx = (device_context *)l_dev_ctx;
    swat_context *context = (swat_context *)lctx;

    cl_region *reallocated = is_cached(broadcastId, rddid, partitionid,
            offsetid, componentid, dev_ctx, context);
    if (broadcastId >= 0) {
        if (reallocated) {
#ifdef VERBOSE
            fprintf(stderr, "caching broadcast %ld %d\n", broadcastId, componentid);
#endif
            add_pending_region_arg(context, index, true, persistent, false, reallocated);
        } else {
            broadcast_id uuid(broadcastId, componentid);
            void *arr = (void *)buffer;
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                    context, dev_ctx, broadcastId, rddid, persistent, blocking);
            if (new_region == NULL) return false;

            lock_bcast_cache(dev_ctx);
            (*dev_ctx->broadcast_cache)[uuid] = new_region;
#ifdef VERBOSE
            fprintf(stderr, "adding broadcast %ld %d to cache\n", broadcastId, componentid);
#endif
            unlock_bcast_cache(dev_ctx);
        }
    } else if (rddid >= 0) {
        if (reallocated) {
#ifdef VERBOSE
            fprintf(stderr, "caching rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
#endif
            add_pending_region_arg(context, index, true, persistent, false, reallocated);
        } else {
            rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid);
            void *arr = (void *)buffer;
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index, context,
                    dev_ctx, broadcastId, rddid, persistent, blocking);
            if (new_region == NULL) return false;

            lock_rdd_cache(uuid);
            update_rdd_cache(uuid, new_region, dev_ctx->device_index);
#ifdef VERBOSE
            fprintf(stderr, "adding rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
#endif
            unlock_rdd_cache(uuid);
        }
    } else {
        assert(reallocated == NULL);
        ASSERT(rddid < 0 && broadcastId < 0);
        void *arr = (void *)buffer;
        cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                context, dev_ctx, broadcastId, rddid, persistent, blocking);
        if (new_region == NULL) return false;
    }

    EXIT_TRACE("setNativeArrayArg");
    return true;
}

JNI_JAVA(jboolean, OpenCLBridge, setArrayArgImpl)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index,
         jobject argObj, jint argLength, jint argEleSize, jlong broadcastId, jint rddid,
         jint partitionid, jint offsetid, jint componentid, jboolean persistent) {
    ENTER_TRACE("setArrayArg");
    jarray arg = (jarray)argObj;

    jsize len = argLength * argEleSize;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    swat_context *context = (swat_context *)lctx;

    cl_region *reallocated = is_cached(broadcastId, rddid, partitionid,
            offsetid, componentid, dev_ctx, context);

    if (broadcastId >= 0) {
        if (reallocated) {
            TRACE_MSG("caching broadcast %ld %d\n", broadcastId, componentid);
            add_pending_region_arg(context, index, true, persistent, false, reallocated);
        } else {
            broadcast_id uuid(broadcastId, componentid);
            void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL);
            CHECK_JNI(arr)
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                    context, dev_ctx, broadcastId, rddid, persistent, true);
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT);
            if (new_region == NULL) return false;

            lock_bcast_cache(dev_ctx);
            (*dev_ctx->broadcast_cache)[uuid] = new_region;
            TRACE_MSG("adding broadcast %ld %d to cache\n", broadcastId, componentid);
            unlock_bcast_cache(dev_ctx);
        }
    } else if (rddid >= 0) {
        if (reallocated) {
            TRACE_MSG("caching rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
            add_pending_region_arg(context, index, true, persistent, false, reallocated);
        } else {
            rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid);
            void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL);
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index, context,
                    dev_ctx, broadcastId, rddid, persistent, true);
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT);
            if (new_region == NULL) return false;

            lock_rdd_cache(uuid);
            update_rdd_cache(uuid, new_region, dev_ctx->device_index);
            TRACE_MSG("adding rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
            unlock_rdd_cache(uuid);
        }
    } else {
        assert(reallocated == NULL);
        ASSERT(rddid < 0 && broadcastId < 0);
        void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL);
        CHECK_JNI(arr)
        cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                context, dev_ctx, broadcastId, rddid, persistent, true);
        jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT);
        if (new_region == NULL) return false;
    }
    EXIT_TRACE("setArrayArg");
    return true;
}

JNI_JAVA(jboolean, OpenCLBridge, tryCache)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx,
         jint index, jlong broadcastId, jint rddid, jint partitionid,
         jint offsetid, jint componentid, jint ncomponents, jboolean persistent) {
    ENTER_TRACE("tryCache");

    device_context *dev_ctx = (device_context *)l_dev_ctx;
    swat_context *context = (swat_context *)lctx;
    bool all_succeed = true;
    int c = 0;
    const int pending_arguments_before = context->accumulated_arguments_len;

#ifdef VERBOSE
    fprintf(stderr, "tryCache: index=%d broadcast=%ld rdd=%d partition=%d "
            "offset=%d component=%d ncomponents=%d\n", index, broadcastId,
            rddid, partitionid, offsetid, componentid, ncomponents);
#endif
    if (broadcastId >= 0) {
        ASSERT(rddid < 0 && componentid >= 0);

        lock_bcast_cache(dev_ctx);
        
        while (all_succeed && c < ncomponents) {
            broadcast_id uuid(broadcastId, componentid + c);
            cl_region *region = NULL;
            map<broadcast_id, cl_region *>::iterator found =
                dev_ctx->broadcast_cache->find(uuid);
            if (found != dev_ctx->broadcast_cache->end()) {
                region = found->second;
            }
            bool reallocated = (region && re_allocate_cl_region(region,
                        dev_ctx->device_index));
            if (reallocated) {
#ifdef VERBOSE
                fprintf(stderr, "%d: caching broadcast %ld %d\n",
                        context->host_thread_index, broadcastId,
                        componentid + c);
#endif
                // A cached item should never be persistent
                add_pending_region_arg(context, index + c, true, persistent, false, region);
            } else {
#ifdef VERBOSE
                fprintf(stderr, "%d: failed to try-cache broadcast %ld %d\n",
                        context->host_thread_index, broadcastId,
                        componentid + c);
#endif
                remove_from_broadcast_cache_if_present(uuid, dev_ctx);
                all_succeed = false;
            }
            c++;
        }
    } else if (rddid >= 0) {
        ASSERT(broadcastId < 0 && partitionid >= 0 && offsetid >= 0 && componentid >= 0);
        lock_rdd_cache_by_partition(partitionid);

        while (all_succeed && c < ncomponents) {
            rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid + c);
            cl_region *region = check_rdd_cache(uuid, dev_ctx);
            bool reallocated = (region && re_allocate_cl_region(region,
                        dev_ctx->device_index));
            if (reallocated) {
#ifdef VERBOSE
                fprintf(stderr, "%d: caching rdd=%d partition=%d offset=%d "
                        "component=%d\n", context->host_thread_index, rddid,
                        partitionid, offsetid, componentid + c);
#endif
                add_pending_region_arg(context, index + c, true, persistent, false, region);
            } else {
#ifdef VERBOSE
                fprintf(stderr, "%d: failed to try-cache rdd=%d partition=%d "
                        "offset=%d component=%d\n", context->host_thread_index,
                        rddid, partitionid, offsetid, componentid + c);
#endif
                remove_from_rdd_cache_if_present(uuid, dev_ctx->device_index);
                all_succeed = false;
            }
            c++;
        }
    } else {
        EXIT_TRACE("tryCache");
        return false;
    }

    if (!all_succeed) {
        /*
         * If they didn't all succeed, then free the ones that didn't. In
         * the future, it may be possible to partially re-use cached
         * components mixed with re-serialized components but that would
         * increase the complexity of the calling code so for now it's just
         * a TODO.
         */
        for (int i = pending_arguments_before;
                i < context->accumulated_arguments_len; i++) {
            arg_value *curr = context->accumulated_arguments + i;
            free_cl_region(curr->val.region, curr->keep);
        }
        context->accumulated_arguments_len = pending_arguments_before;
    }

    if (broadcastId >= 0) {
        unlock_bcast_cache(dev_ctx);
    } else if (rddid >= 0) {
        unlock_rdd_cache_by_partition(partitionid);
    }

    EXIT_TRACE("tryCache");
    return all_succeed;
}

JNI_JAVA(void, OpenCLBridge, releaseAllPendingRegions)(JNIEnv *jenv, jclass clazz,
        jlong lctx) {
    ENTER_TRACE("releaseAllPendingRegions");
    swat_context *context = (swat_context *)lctx;

    for (int i = 0; i < context->accumulated_arguments_len; i++) {
        arg_value *curr = context->accumulated_arguments + i;
        if (curr->type == REGION && curr->val.region) {
            cl_region *region = curr->val.region;
#ifdef VERBOSE
            fprintf(stderr, "clalloc: releaseAllPendingRegions freeing "
                    "region=%p keep=%s offset=%lu size=%lu\n", region,
                    curr->keep ? "true" : "false", region->offset, region->size);
#endif
            free_cl_region(region, curr->keep);
        }
    }

    context->accumulated_arguments_len = 0;
    EXIT_TRACE("releaseAllPendingRegions");
}

JNI_JAVA(jboolean, OpenCLBridge, setArgUnitialized)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum,
         jlong size, jboolean persistent) {
    ENTER_TRACE("setArgUnitialized");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_region *region = get_mem(context, dev_ctx, argnum, size, -1, -1,
            persistent);
    if (region == NULL) return false;

#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to an unitialized value with size %ld\n",
            argnum, size);
#endif

    EXIT_TRACE("setArgUnitialized");
    return true;
}

JNI_JAVA(void, OpenCLBridge, setNullArrayArg)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint argnum) {
    ENTER_TRACE("setNullArrayArg");
    swat_context *context = (swat_context *)lctx;
    add_pending_region_arg(context, argnum, false, false, false, NULL);
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a null value\n", argnum);
#endif
    EXIT_TRACE("setNullArrayArg");
}

static heap_context *look_for_free_heap_context(device_context *dev_ctx) {
    if (dev_ctx->heap_cache_head) {
        heap_context *result = dev_ctx->heap_cache_head;
        dev_ctx->heap_cache_head = result->next;
        result->next = NULL;

        if (dev_ctx->heap_cache_head == NULL) {
            ASSERT(dev_ctx->heap_cache_tail == result);
            dev_ctx->heap_cache_tail = NULL;
        }
        return result;
    } else {
        return NULL;
    }
}

// TODO dynamically allocate heaps?
static heap_context *acquireHeapImpl(swat_context *ctx, device_context *dev_ctx,
        int heapStartArgnum) {
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int err = pthread_mutex_lock(&dev_ctx->heap_cache_lock);
#ifdef PROFILE_LOCKS
    dev_ctx->heap_cache_lock_contention += (get_clock_gettime_ns() - start);
#endif
    ASSERT(err == 0);

    heap_context *mine = look_for_free_heap_context(dev_ctx);
    if (mine == NULL) {
        mine = (heap_context *)malloc(sizeof(heap_context));
        CHECK_ALLOC(mine);
        createHeapContext(mine, dev_ctx, dev_ctx->heap_size);

        ASSERT(dev_ctx->heap_cache_head == NULL);
        ASSERT(dev_ctx->heap_cache_tail == NULL);
        dev_ctx->n_heaps += 1;
        fprintf(stderr, "Had to create new heap on device %d, nheaps = %d\n", dev_ctx->device_index, dev_ctx->n_heaps);
    }

#ifdef PROFILE_LOCKS
    dev_ctx->heap_cache_blocked += (get_clock_gettime_ns() - start);
#endif

    err = pthread_mutex_unlock(&dev_ctx->heap_cache_lock);
    ASSERT(err == 0);

    return mine;
}

JNI_JAVA(void, OpenCLBridge, cleanupArguments)(JNIEnv *jenv, jclass clazz,
        jlong l_ctx) {
    ENTER_TRACE("cleanupArguments");
    swat_context *context = (swat_context *)l_ctx;

    for (int a = 0; a < context->accumulated_arguments_len; a++) {
        arg_value *val = context->accumulated_arguments + a;
        if (val->type == REGION) {
            cl_region *region = val->val.region;
#ifdef VERBOSE
            fprintf(stderr, "clalloc: cleanupArguments freeing region=%p "
                    "keep=%s offset=%lu size=%lu\n", region,
                    val->keep ? "true" : "false", region->offset, region->size);
#endif
            free_cl_region(region, val->keep);
        }
    }

    context->accumulated_arguments_len = 0;
    EXIT_TRACE("cleanupArguments");
}

static void add_to_global_arguments(arg_value *val, swat_context *ctx) {
    ASSERT(ctx->global_arguments_len <= ctx->global_arguments_capacity);
    if (ctx->global_arguments_len == ctx->global_arguments_capacity) {
        const int new_capacity = ctx->global_arguments_capacity * 2;
        ctx->global_arguments = (arg_value *)realloc(ctx->global_arguments,
                new_capacity * sizeof(arg_value));
        CHECK_ALLOC(ctx->global_arguments);
        ctx->global_arguments_capacity = new_capacity;
    }

    memcpy(ctx->global_arguments + ctx->global_arguments_len, val, sizeof(arg_value));
    ctx->global_arguments_len = ctx->global_arguments_len + 1;
}

static void setKernelArgument(arg_value *val, swat_context *context,
        device_context *dev_ctx) {
    const int index = val->index;

    switch (val->type) {
        case REGION:
            cl_region *region = val->val.region;
            if (region) {
                cl_mem mem = val->val.region->sub_mem;
#ifdef VERBOSE
                fprintf(stderr, "setKernelArgument: thread=%d ctx=%p index=%d "
                        "mem=%p\n", context->host_thread_index, context, index,
                        mem);
#endif

                CHECK(clSetKernelArg(context->kernel, index, sizeof(mem),
                            &mem));
#ifdef BRIDGE_DEBUG
                (*context->debug_arguments)[index] = new kernel_arg(mem,
                        val->val.region->size, dev_ctx);
#endif
            } else {
#ifdef VERBOSE
                fprintf(stderr, "setKernelArgument: thread=%d ctx=%p index=%d "
                        "mem=NULL\n", context->host_thread_index, context,
                        index);
#endif
                cl_mem none = 0x0;
                CHECK(clSetKernelArg(context->kernel, index, sizeof(none), &none));
#ifdef BRIDGE_DEBUG
                (*context->debug_arguments)[index] = new kernel_arg(&none,
                        sizeof(none), false, false);
#endif
            }
            break;
        case INT:
            const int i = val->val.i;
#ifdef VERBOSE
            fprintf(stderr, "setKernelArgument: thread=%d ctx=%p index=%d "
                    "val=%d\n", context->host_thread_index, context, index, i);
#endif
            CHECK(clSetKernelArg(context->kernel, index, sizeof(i), &i));
#ifdef BRIDGE_DEBUG
                (*context->debug_arguments)[index] = new kernel_arg((void *)&i,
                        sizeof(i), false, false);
#endif
            break;
        case FLOAT:
            const float f = val->val.f;
#ifdef VERBOSE
            fprintf(stderr, "setKernelArgument: thread=%d ctx=%p index=%d "
                    "val=%f\n", context->host_thread_index, context, index, f);
#endif
            CHECK(clSetKernelArg(context->kernel, index, sizeof(f), &f));
#ifdef BRIDGE_DEBUG
                (*context->debug_arguments)[index] = new kernel_arg((void *)&f,
                        sizeof(f), false, false);
#endif
            break;
        case DOUBLE:
            const double d = val->val.d;
#ifdef VERBOSE
            fprintf(stderr, "setKernelArgument: thread=%d ctx=%p index=%d "
                    "val=%f\n", context->host_thread_index, context, index, d);
#endif
            CHECK(clSetKernelArg(context->kernel, index, sizeof(d), &d));
#ifdef BRIDGE_DEBUG
                (*context->debug_arguments)[index] = new kernel_arg((void *)&d,
                        sizeof(d), false, false);
#endif
            break;
        default:
            fprintf(stderr, "setKernelArgument: Unexpected type\n");
            exit(1);
    }
}

JNI_JAVA(void, OpenCLBridge, setupGlobalArguments)(JNIEnv *jenv, jclass clazz,
        jlong l_ctx, jlong l_dev_ctx) {
    swat_context *context = (swat_context *)l_ctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;

    for (int a = 0; a < context->accumulated_arguments_len; a++) {
        arg_value *val = context->accumulated_arguments + a;

        setKernelArgument(val, context, dev_ctx);

        if (val->type == REGION && val->val.region) {
            add_to_global_arguments(val, context);
        }
    }
    context->accumulated_arguments_len = 0;
}

#ifdef BRIDGE_DEBUG
static void save_to_dump_file(swat_context *context, size_t global_size,
        size_t local_size) {
    char filename[256];
    sprintf(filename, "bridge.dump.tid%d.%d", context->host_thread_index,
            context->dump_index);
    int fd = open(filename, O_WRONLY | O_CREAT, O_EXCL | S_IRUSR | S_IWUSR);
    context->dump_index = context->dump_index + 1;

    // Write kernel source to dump file
    safe_write(fd, &global_size, sizeof(global_size));
    safe_write(fd, &local_size, sizeof(local_size));
    safe_write(fd, &context->kernel_src_len, sizeof(context->kernel_src_len));
    safe_write(fd, context->kernel_src, context->kernel_src_len + 1);
    int num_args = context->debug_arguments->size();
    safe_write(fd, &num_args, sizeof(num_args));

    for (map<int, kernel_arg *>::iterator i =
            context->debug_arguments->begin(), e =
            context->debug_arguments->end(); i != e; i++) {
        int arg_index = i->first;
        kernel_arg *arg = i->second;

        safe_write(fd, &arg_index, sizeof(arg_index));
        arg->dump(fd);
    }

    close(fd);
}
#endif

static void heap_copy_callback(cl_event event, cl_int event_command_exec_status,
        void *user_data);
static void copy_kernel_outputs(kernel_context *kernel_ctx, cl_event prev_event);

static void runImpl(kernel_context *kernel_ctx, cl_event prev_event) {
    const int free_index_arg_index = kernel_ctx->heapStartArgnum + 1;
    swat_context *ctx = kernel_ctx->ctx;
    device_context *dev_ctx = kernel_ctx->dev_ctx;
    heap_context *heap_ctx = NULL;
    cl_region *free_index_mem = NULL;
    int *pinned_h_free_index = NULL;

    // Lock kernel to prevent concurrent changes via clSetKernelArg
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int perr = pthread_mutex_lock(&ctx->kernel_lock);
#ifdef PROFILE_LOCKS
    ctx->kernel_lock_contention += (get_clock_gettime_ns() - start);
#endif
    ASSERT(perr == 0);

    if (kernel_ctx->heapStartArgnum >= 0) {
#ifdef VERBOSE
        fprintf(stderr, "thread=%d ctx=%p waiting for heap on device %d\n",
                ctx->host_thread_index, ctx, dev_ctx->device_index);
#endif
        heap_ctx = acquireHeapImpl(ctx, kernel_ctx->dev_ctx,
                kernel_ctx->heapStartArgnum);
        kernel_ctx->curr_heap_ctx = heap_ctx;
#ifdef VERBOSE
        fprintf(stderr, "thread=%d ctx=%p got heap %u on device %d\n",
                ctx->host_thread_index, ctx, heap_ctx->id,
                dev_ctx->device_index);
#endif

        free_index_mem = heap_ctx->free_index;
        pinned_h_free_index = heap_ctx->pinned_h_free_index;

        CHECK(clSetKernelArg(ctx->kernel, kernel_ctx->heapStartArgnum,
                    sizeof(cl_mem), &heap_ctx->heap->sub_mem));
        CHECK(clSetKernelArg(ctx->kernel, kernel_ctx->heapStartArgnum + 1,
                    sizeof(cl_mem), &heap_ctx->free_index->sub_mem));
        CHECK(clSetKernelArg(ctx->kernel, kernel_ctx->heapStartArgnum + 2,
                    sizeof(heap_ctx->heap_size), &heap_ctx->heap_size));
#ifdef BRIDGE_DEBUG
        (*ctx->debug_arguments)[kernel_ctx->heapStartArgnum] = new kernel_arg(
                heap_ctx->heap->sub_mem, heap_ctx->heap_size,
                kernel_ctx->dev_ctx);
        (*ctx->debug_arguments)[kernel_ctx->heapStartArgnum + 1] =
            new kernel_arg(heap_ctx->free_index->sub_mem, sizeof(zero),
                    kernel_ctx->dev_ctx);
        (*ctx->debug_arguments)[kernel_ctx->heapStartArgnum + 2] =
            new kernel_arg(&heap_ctx->heap_size, sizeof(heap_ctx->heap_size),
                    false, false);
#endif

        // Clear the free index of the acquired heap asynchronously
        cl_event free_index_event;
        *pinned_h_free_index = 0;
        CHECK(clEnqueueWriteBuffer(kernel_ctx->dev_ctx->cmd,
                    free_index_mem->sub_mem, CL_FALSE, 0,
                    sizeof(zero), pinned_h_free_index, prev_event ? 1 : 0,
                    prev_event ? &prev_event : NULL,
                    &free_index_event));
#ifdef PROFILE_OPENCL
        add_event_to_list(&kernel_ctx->acc_write_events, free_index_event, "free_index-in",
                &kernel_ctx->acc_write_events_length,
                &kernel_ctx->acc_write_events_capacity);
#endif
        prev_event = free_index_event;
    }

    for (int a = 0; a < kernel_ctx->accumulated_arguments_len; a++) {
        arg_value *curr = kernel_ctx->accumulated_arguments + a;
        setKernelArgument(curr, ctx, kernel_ctx->dev_ctx);
    }

#ifdef BRIDGE_DEBUG
    (*ctx->debug_arguments)[kernel_ctx->iterArgNum] = new kernel_arg(
            &kernel_ctx->iter, sizeof(kernel_ctx->iter), false, false);
    save_to_dump_file(ctx, kernel_ctx->global_size, kernel_ctx->local_size);
#endif

    // Set the current iter of this kernel instance
    CHECK(clSetKernelArg(ctx->kernel, kernel_ctx->iterArgNum,
                sizeof(kernel_ctx->iter), &kernel_ctx->iter));
    // Launch a new invocation of this kernel
    cl_event run_event;
    CHECK(clEnqueueNDRangeKernel(kernel_ctx->dev_ctx->cmd,
                ctx->kernel, 1, NULL, &kernel_ctx->global_size,
                &kernel_ctx->local_size, prev_event ? 1 : 0,
                prev_event ? &prev_event : NULL, &run_event));
#ifdef PROFILE_OPENCL
        add_event_to_list(&kernel_ctx->acc_write_events, run_event, "run",
                &kernel_ctx->acc_write_events_length,
                &kernel_ctx->acc_write_events_capacity);
#endif

    perr = pthread_mutex_unlock(&ctx->kernel_lock);
    ASSERT(perr == 0);

    // Increment the number of kernel retries/iters
    kernel_ctx->iter = kernel_ctx->iter + 1;

    if (kernel_ctx->heapStartArgnum >= 0) {
        /*
         * After the kernel, copy back the heap free index and then run
         * heap_copy_callback on completion.
         */
        cl_event copy_back_event;
        CHECK(clEnqueueReadBuffer(kernel_ctx->dev_ctx->cmd,
                    free_index_mem->sub_mem, CL_FALSE, 0,
                    sizeof(zero), pinned_h_free_index, 1,
                    &run_event, &copy_back_event));
#ifdef PROFILE_OPENCL
        add_event_to_list(&kernel_ctx->acc_write_events, copy_back_event, "free_index-out",
                &kernel_ctx->acc_write_events_length,
                &kernel_ctx->acc_write_events_capacity);
#endif

        CHECK(clSetEventCallback(copy_back_event, CL_COMPLETE,
                    heap_copy_callback, kernel_ctx));
    } else {
        copy_kernel_outputs(kernel_ctx, run_event);
    }
}

// static void release_device_heap_callback(cl_event event,
//         cl_int event_command_exec_status, void *user_data) {
//     ASSERT(event_command_exec_status == CL_COMPLETE);
//     heap_context *heap_ctx = (heap_context *)user_data;
//     device_context *dev_ctx = heap_ctx->dev_ctx;
// 
// 
// 
//     err = pthread_mutex_unlock(&dev_ctx->heap_cache_lock);
//     ASSERT(err == 0);
// }

static cl_region *find_kernel_specific_argument_for(kernel_context *kernel_ctx,
        int index) {
    const int args_len = kernel_ctx->accumulated_arguments_len;

    arg_value *found = NULL;
    for (int i = 0; i < args_len && found == NULL; i++) {
        arg_value *curr = kernel_ctx->accumulated_arguments + i;
        if (curr->index == index) {
            found = curr;
        }
    }
    ASSERT(found);
    ASSERT(found->type == REGION);
    ASSERT(found->val.region);
    return found->val.region;
}

static void finally_done_callback(cl_event event,
        cl_int event_command_exec_status, void *user_data) {
    ENTER_TRACE("finally_done_callback");
    ASSERT(event_command_exec_status == CL_COMPLETE);
    kernel_context *kernel_ctx = (kernel_context *)user_data;
    swat_context *ctx = kernel_ctx->ctx;
#ifdef VERBOSE
    fprintf(stderr, "finally_done_callback: thread=%d ctx=%p seq=%d\n",
            ctx->host_thread_index, ctx, kernel_ctx->seq_no);
#endif

#ifdef PROFILE_OPENCL
    // Print results
    fprintf(stderr, "OpenCL PROFILING RESULTS, host thread=%d device=%d\n",
            ctx->host_thread_index, kernel_ctx->dev_ctx->device_index);
    for (int i = 0; i < kernel_ctx->acc_write_events_length; i++) {
        cl_ulong queued, submitted, started, finished;
        CHECK(clGetEventProfilingInfo((kernel_ctx->acc_write_events)[i].event,
                    CL_PROFILING_COMMAND_QUEUED, sizeof(queued), &queued, NULL));
        CHECK(clGetEventProfilingInfo((kernel_ctx->acc_write_events)[i].event,
                    CL_PROFILING_COMMAND_SUBMIT, sizeof(submitted), &submitted,
                    NULL));
        CHECK(clGetEventProfilingInfo((kernel_ctx->acc_write_events)[i].event,
                    CL_PROFILING_COMMAND_START, sizeof(started), &started,
                    NULL));
        CHECK(clGetEventProfilingInfo((kernel_ctx->acc_write_events)[i].event,
                    CL_PROFILING_COMMAND_END, sizeof(finished), &finished,
                    NULL));
        fprintf(stderr, "  %d: %s : %lu ns total (started = %llu, queued -> submitted %lu ns, "
                "submitted -> started %lu ns, started -> finished %lu ns)\n",
                i, (kernel_ctx->acc_write_events)[i].label, finished - queued,
                (kernel_ctx->acc_write_events)[i].timestamp - app_start_time,
                submitted - queued, started - submitted, finished - started);
        free((kernel_ctx->acc_write_events)[i].label);
    }
    free(kernel_ctx->acc_write_events);
#endif

    kernel_ctx->next = NULL;
#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int perr = pthread_mutex_lock(&ctx->completed_kernels_lock);
#ifdef PROFILE_LOCKS
    ctx->completed_kernels_lock_contention += (get_clock_gettime_ns() - start);
#endif
    ASSERT(perr == 0);

    kernel_ctx->next = NULL;
    if (ctx->completed_kernels == NULL) {
        ctx->completed_kernels = kernel_ctx;
    } else {
        kernel_context *curr = ctx->completed_kernels;
        while (curr->next != NULL) {
            curr = curr->next;
        }
        curr->next = kernel_ctx;
    }

    perr = pthread_cond_signal(&ctx->completed_kernels_cond);
    ASSERT(perr == 0);

    perr = pthread_mutex_unlock(&ctx->completed_kernels_lock);
    ASSERT(perr == 0);
    EXIT_TRACE("finally_done_callback");
}

static void copy_kernel_outputs(kernel_context *kernel_ctx,
        cl_event prev_event) {
    swat_context *ctx = kernel_ctx->ctx;
    device_context *dev_ctx = kernel_ctx->dev_ctx;

#ifdef VERBOSE
    fprintf(stderr, "copy_kernel_outputs: thread=%d ctx=%p seq=%d\n",
            ctx->host_thread_index, ctx, kernel_ctx->seq_no);
#endif

    const int args_len = kernel_ctx->accumulated_arguments_len;
    for (int i = 0; i < args_len; i++) {
        arg_value *curr = kernel_ctx->accumulated_arguments + i;
        /*
         * TODO in some cases this may be inefficient, as copying out the whole
         * output array may be unnecessary
         */
        if (curr->type == REGION && curr->copy_out) {
            cl_region *region = curr->val.region;
            void *pinned = fetch_pinned(region);

            cl_event next_event;
            CHECK(clEnqueueReadBuffer(dev_ctx->cmd, region->sub_mem, CL_FALSE,
                        0, region->size, pinned, 1, &prev_event, &next_event));
#ifdef PROFILE_OPENCL
            add_event_to_list(&kernel_ctx->acc_write_events, next_event, "out",
                    &kernel_ctx->acc_write_events_length,
                    &kernel_ctx->acc_write_events_capacity);
#endif
            prev_event = next_event;
        }
    }

    CHECK(clSetEventCallback(prev_event, CL_COMPLETE, finally_done_callback,
                kernel_ctx));
}

/*
 * heap_copy_callback is called following a successful kernel launch, including
 * the asynchronous transfer back of the heap's free index after the kernel.
 * heap_copy_callback is responsible for transferring the contents of the
 * current heap out of the device and ensuring the device heap object is
 * released afterwards. It also handles the success or failure of the kernel. If
 * the kernel succeeded, the host application is notified through the
 * completed_kernels queue. Otherwise, a retry of this kernel is launched using
 * runImpl.
 */
static void heap_copy_callback(cl_event event, cl_int event_command_exec_status,
        void *user_data) {
    ASSERT(event_command_exec_status == CL_COMPLETE);

    kernel_context *kernel_ctx = (kernel_context *)user_data;
    heap_context *heap_ctx = (heap_context *)kernel_ctx->curr_heap_ctx;
    swat_context *ctx = kernel_ctx->ctx;
    device_context *dev_ctx = kernel_ctx->dev_ctx;
#ifdef VERBOSE
    fprintf(stderr, "heap_copy_callback: thread=%d ctx=%p seq=%d\n",
            ctx->host_thread_index, ctx, kernel_ctx->seq_no);
#endif

    const int free_index = *(heap_ctx->pinned_h_free_index);
    const size_t available_bytes =
        (free_index > heap_ctx->heap_size ? heap_ctx->heap_size : free_index);

    cl_event heap_event;
    CHECK(clEnqueueReadBuffer(dev_ctx->cmd, heap_ctx->heap->sub_mem, CL_FALSE,
                0, available_bytes, heap_ctx->pinned_h_heap, 0, NULL, &heap_event));
#ifdef PROFILE_OPENCL
    add_event_to_list(&kernel_ctx->acc_write_events, heap_event, "heap",
            &kernel_ctx->acc_write_events_length,
            &kernel_ctx->acc_write_events_capacity);
#endif

    (kernel_ctx->heaps)[kernel_ctx->n_heap_ctxs].heap_ctx = heap_ctx;
    (kernel_ctx->heaps)[kernel_ctx->n_heap_ctxs].size = available_bytes;
    (kernel_ctx->heap_copy_back_events)[kernel_ctx->n_heap_ctxs] = heap_event;
    kernel_ctx->n_heap_ctxs = kernel_ctx->n_heap_ctxs + 1;

    // fprintf(stderr, "free_index=%d heap_size=%u\n", free_index, heap_ctx->heap_size);

    if (free_index > heap_ctx->heap_size) {
        // If need kernel restart
        runImpl(kernel_ctx, NULL);
    } else {

        kernel_complete_flag *done_flag = kernel_ctx->done_flag;
        int perr = pthread_mutex_lock(&done_flag->lock);
        ASSERT(perr == 0);
        done_flag->done = 1;
        perr = pthread_cond_signal(&done_flag->cond);
        ASSERT(perr == 0);
        perr = pthread_mutex_unlock(&done_flag->lock);
        ASSERT(perr == 0);

        copy_kernel_outputs(kernel_ctx, heap_event);
    }
}

static kernel_context *find_matching_kernel_ctx(swat_context *ctx, int seq_no, int tid) {
    kernel_context *prev = NULL;
    kernel_context *curr = ctx->completed_kernels;

#ifdef VERBOSE
    fprintf(stderr, "find_matching_kernel_ctx: thread=%d ctx=%p looking for "
            "seq=%d\n", tid, ctx, seq_no);
    fprintf(stderr, "find_matching_kernel_ctx:   thread=%d ctx=%p curr=%p "
            "curr->seq_no=%d\n", tid, ctx, curr, curr ? curr->seq_no : -1);
#endif
    while (curr != NULL && curr->seq_no != seq_no) {
        prev = curr;
        curr = curr->next;
#ifdef VERBOSE
        fprintf(stderr, "find_matching_kernel_ctx:   thread=%d ctx=%p curr=%p "
                "curr->seq_no=%d\n", tid, ctx, curr, curr ? curr->seq_no : -1);
#endif
    }

    // Remove from singly linked list on success
    if (curr != NULL) {
        if (prev == NULL) {
            ctx->completed_kernels = curr->next;
        } else {
            prev->next = curr->next;
        }
    }
    return curr;
}

JNI_JAVA(void, OpenCLBridge, cleanupKernelContext)(JNIEnv *jenv, jclass clazz,
        jlong l_kernel_ctx) {
    kernel_context *kernel_ctx = (kernel_context *)l_kernel_ctx;
    swat_context *ctx = kernel_ctx->ctx;
    device_context *dev_ctx = kernel_ctx->dev_ctx;

#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int err = pthread_mutex_lock(&dev_ctx->heap_cache_lock);
#ifdef PROFILE_LOCKS
    dev_ctx->heap_cache_lock_contention += (get_clock_gettime_ns() - start);
#endif
    ASSERT(err == 0);

    for (int i = 0; i < kernel_ctx->n_heap_ctxs; i++) {
        heap_context *heap_ctx = (kernel_ctx->heaps)[i].heap_ctx;

        heap_ctx->next = NULL;
        if (dev_ctx->heap_cache_tail) {
            ASSERT(dev_ctx->heap_cache_head);
            dev_ctx->heap_cache_tail->next = heap_ctx;
            dev_ctx->heap_cache_tail = heap_ctx;
        } else {
            ASSERT(dev_ctx->heap_cache_head == NULL);
            dev_ctx->heap_cache_head = heap_ctx;
            dev_ctx->heap_cache_tail = heap_ctx;
        }
#ifdef VERBOSE
        fprintf(stderr, "releasing heap %d on device %d\n", heap_ctx->id,
                dev_ctx->device_index);
#endif

        // int perr = pthread_mutex_lock(&heap_ctx->h_heap_lock);
        // ASSERT(perr == 0);
        // heap_ctx->h_heap_in_use = 0;
        // perr = pthread_cond_signal(&heap_ctx->h_heap_cond);
        // ASSERT(perr == 0);
        // perr = pthread_mutex_unlock(&heap_ctx->h_heap_lock);
        // ASSERT(perr == 0);
    }

    err = pthread_mutex_unlock(&dev_ctx->heap_cache_lock);
    ASSERT(err == 0);

    free(kernel_ctx->heaps);
    free(kernel_ctx->heap_copy_back_events);
    free(kernel_ctx);
}

JNI_JAVA(jlong, OpenCLBridge, waitForFinishedKernel)(JNIEnv *jenv, jclass clazz,
        jlong lctx, jlong l_dev_ctx, jint seq_no) {
    ENTER_TRACE("waitForFinishedKernel");
    swat_context *ctx = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;

#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int perr = pthread_mutex_lock(&ctx->completed_kernels_lock);
#ifdef PROFILE_LOCKS
    ctx->completed_kernels_lock_contention += (get_clock_gettime_ns() - start);
#endif
    ASSERT(perr == 0);

    kernel_context *mine = NULL;
    while ((mine = find_matching_kernel_ctx(ctx, seq_no, ctx->host_thread_index)) == NULL) {
        perr = pthread_cond_wait(&ctx->completed_kernels_cond,
                &ctx->completed_kernels_lock);
        ASSERT(perr == 0);
    }
    ASSERT(mine);

#ifdef PROFILE_LOCKS
    const unsigned long long elapsed = (get_clock_gettime_ns() - start);
    ctx->completed_kernels_blocked += elapsed;
#endif
    perr = pthread_mutex_unlock(&ctx->completed_kernels_lock);
    ASSERT(perr == 0);

    /*
     * There may be no heap copy back events if we're running a kernel that
     * performs no dynamic memory allocations.
     */
    if (mine->n_heap_ctxs > 0) {
        CHECK(clWaitForEvents(mine->n_heap_ctxs, mine->heap_copy_back_events));
    }

    /*
     * At this point we know the kernel and all of its transfers out (including
     * heap contents and output buffers) have completed. We clean up any OpenCL
     * resources that we're still holding. All heap contexts will have been
     * released by the release_device_heap_callback callback following their
     * copy out to native buffers, so we only need to concern ourselves with any
     * explicitly allocated OpenCL buffers from clalloc.
     */
    cl_allocator *allocator = NULL;
    for (int a = 0; a < mine->accumulated_arguments_len; a++) {
        arg_value *curr = mine->accumulated_arguments + a;
        if (curr->type == REGION && curr->val.region) {
            cl_region *region = curr->val.region;

            if (allocator) {
                ASSERT(allocator == region->grandparent->allocator);
            } else {
                allocator = region->grandparent->allocator;
            }

#ifdef VERBOSE
            fprintf(stderr, "clalloc: waitForFinishedKernel freeing region=%p "
                    "keep=%s offset=%lu size=%lu\n", region,
                    curr->keep ? "true" : "false", region->offset, region->size);
#endif
            /*
             * Don't free regions owned by either a native input buffer or
             * native output buffer. These buffers have a lifetime longer than
             * this kernel, so we don't want to release them back to clalloc.
             */
            if (!curr->dont_free) {
                free_cl_region(region, curr->keep);
            }
        }
    }
    free(mine->accumulated_arguments);
    mine->accumulated_arguments_len = 0;

#ifdef VERBOSE
#ifdef VERY_VERBOSE
    fprintf(stderr, "After finishing kernel:\n");
    print_allocator(dev_ctx->allocator, ctx->host_thread_index);
#endif
#endif

    EXIT_TRACE("waitForFinishedKernel");
    return (jlong)mine;
}

JNI_JAVA(jlong, OpenCLBridge, run)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx,
         jint range, jint local_size_in, jint iterArgNum,
         jint heapArgStart, jint maxHeaps, jint outputBufferId) {
    ENTER_TRACE("run");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    const size_t local_size = local_size_in;
    const size_t global_size = range + (local_size - (range % local_size));

#ifdef VERBOSE
    fprintf(stderr, "Host thread %d launching kernel on OpenCL device %s, "
            "range=%d, global_size=%lu, local_size=%lu\n",
            context->host_thread_index, get_device_name(dev_ctx->dev), range,
            global_size, local_size);
    fprintf(stderr, "On device %d have %lu free bytes\n", dev_ctx->device_index,
            count_free_bytes(dev_ctx->allocator));
#ifdef VERY_VERBOSE
    print_allocator(dev_ctx->allocator, context->host_thread_index);
#endif
#endif

    /*
     * Configure a kernel context representing all of the work necessary to
     * complete processing of a single batch of items, including possible kernel
     * retries requiring multiple heaps.
     */
    kernel_context *kernel_ctx = (kernel_context *)malloc(sizeof(kernel_context));
    CHECK_ALLOC(kernel_ctx);
    kernel_ctx->ctx = context;
    kernel_ctx->dev_ctx = dev_ctx;
    kernel_ctx->curr_heap_ctx = NULL;
    kernel_ctx->heaps = (saved_heap *)malloc(maxHeaps * sizeof(saved_heap));
    kernel_ctx->heap_copy_back_events = (cl_event *)malloc(maxHeaps * sizeof(cl_event));
    kernel_ctx->n_heap_ctxs = 0;
    kernel_ctx->heapStartArgnum = heapArgStart;
    kernel_ctx->n_loaded = range;
    kernel_ctx->local_size = local_size;
    kernel_ctx->global_size = global_size;
    kernel_ctx->seq_no = context->run_seq_no;
    kernel_ctx->iter = 0;
    kernel_ctx->iterArgNum = iterArgNum;
    kernel_ctx->next = NULL;
    kernel_ctx->accumulated_arguments = context->accumulated_arguments;
    kernel_ctx->accumulated_arguments_len = context->accumulated_arguments_len;
    kernel_ctx->output_buffer_id = outputBufferId;

    kernel_complete_flag *done_flag = (kernel_complete_flag *)malloc(
            sizeof(kernel_complete_flag));
    CHECK_ALLOC(done_flag);
    done_flag->done = 0;
    int perr = pthread_mutex_init(&done_flag->lock, NULL);
    ASSERT(perr == 0);
    perr = pthread_cond_init(&done_flag->cond, NULL);
    ASSERT(perr == 0);
    done_flag->host_thread_index = context->host_thread_index;
    done_flag->seq = context->run_seq_no;
    kernel_ctx->done_flag = done_flag;

    context->accumulated_arguments = (arg_value *)malloc(
            context->accumulated_arguments_capacity * sizeof(arg_value));
    CHECK_ALLOC(context->accumulated_arguments);
    context->accumulated_arguments_len = 0;

    context->run_seq_no = context->run_seq_no + 1;

#ifdef VERBOSE
    fprintf(stderr, "thread=%d ctx=%p finished setting up kernel context for "
            "seq=%d before run\n", context->host_thread_index, context,
            kernel_ctx->seq_no);
#endif

#ifdef PROFILE_OPENCL
    kernel_ctx->acc_write_events = context->acc_write_events;
    kernel_ctx->acc_write_events_length = context->acc_write_events_length;
    kernel_ctx->acc_write_events_capacity = context->acc_write_events_capacity;
    context->acc_write_events = (event_info *)malloc(
            context->acc_write_events_capacity * sizeof(event_info));
    CHECK_ALLOC(context->acc_write_events);
    context->acc_write_events_length = 0;
#endif

    // Launch the asynchronous processing of this kernel instance
    runImpl(kernel_ctx, context->last_write_event);
#ifdef VERBOSE
    fprintf(stderr, "thread=%d ctx=%p completed runImpl for seq=%d\n",
            context->host_thread_index, context, kernel_ctx->seq_no);
#endif
    context->last_write_event = NULL;

    bump_time(dev_ctx->allocator);
#ifdef VERBOSE
    fprintf(stderr, "thread=%d ctx=%p exiting runImpl\n",
            context->host_thread_index, context);
#endif

    EXIT_TRACE("run");
    return (jlong)done_flag;
}

JNI_JAVA(void, OpenCLBridge, nativeMemcpy)(JNIEnv *jenv, jclass clazz,
        jlong dstBuffer, jint dstOffset, jlong srcBuffer, jint srcOffset, jint nbytes) {
    ENTER_TRACE("nativeMemcpy");
    char *dst = (char *)dstBuffer;
    char *src = (char *)srcBuffer;
    memcpy(dst + dstOffset, src + srcOffset, nbytes);
    EXIT_TRACE("nativeMemcpy");
}

/*
 * 1.  buffer is the native buffer that we want to serialize 1 or more dense
 *     vectors into (strided).
 * 2.  bufferPosition is the base offset of the newly strided chunk, in elements.
 * 3.  bufferCapacity is the number of elements that can be fit into buffer in
 *     total.
 * 4.  sizesBuffer is a native buffer to store the length of each serialized
 *     buffer into.
 * 5.  offsetsBuffer is a native buffer to store the offsets of each serialized
 *     buffer into, as the number of elements from the start of buffer that the
 *     vector starts at.
 * 6.  buffered is the number of vectors already buffered into buffer.
 * 7.  vectorCapacity is the number of vectors we can store (which is equal to
 *     the length of sizesBuffer and offsetsBuffer).
 * 8.  vectors is a chunk of DenseVector objects to serialize.
 * 9.  vectorSizes is an array storing the length of each DenseVector in vectors,
 *     to save on overhead from having to make a call back to the JVM for each
 *     vector's size.
 * 10. nToSerialize is the number of vectors actually stored in vectors.
 * 11. tiling is the stride to use when serializing the buffers.
 */
JNI_JAVA(jint, OpenCLBridge, serializeStridedDenseVectorsToNativeBuffer)
        (JNIEnv *jenv, jclass clazz, jlong buffer, jint bufferPosition,
         jlong bufferCapacity, jlong sizesBuffer, jlong offsetsBuffer,
         jint buffered, jint vectorCapacity, jobjectArray vectors,
         jintArray vectorSizes, jint nToSerialize, jint tiling) {
    ENTER_TRACE("serializeStridedDenseVectorsToNativeBuffer");
    jenv->EnsureLocalCapacity(nToSerialize);

    double *serialized = (double *)buffer;
    int *sizes = (int *)sizesBuffer;
    int *offsets = (int *)offsetsBuffer;

    int *vectorSizesPtr = (int *)jenv->GetPrimitiveArrayCritical(vectorSizes, NULL);
    CHECK_JNI(vectorSizesPtr);

    int nSerialized;
    for (nSerialized = 0; nSerialized < nToSerialize; nSerialized++) {
        const long offset = bufferPosition + nSerialized;
        jobject vector = jenv->GetObjectArrayElement(vectors, nSerialized);
        CHECK_JNI(vector);

        const jint vectorSize = vectorSizesPtr[nSerialized];
        const long lastElement = offset + ((vectorSize - 1) * tiling);
        if (buffered + nSerialized >= vectorCapacity || lastElement >= bufferCapacity) {
            break;
        }

        // double[] array backing the dense vector
        jarray vectorArray = (jarray)jenv->CallObjectMethod(vector,
                denseVectorValuesMethod);
        CHECK_JNI(vectorArray);
        double *vectorArrayValues = (double *)jenv->GetPrimitiveArrayCritical(
                vectorArray, NULL);
        CHECK_JNI(vectorArrayValues);

        for (int j = 0; j < vectorSize; j++) {
            serialized[offset + (j * tiling)] = vectorArrayValues[j];
        }

        jenv->ReleasePrimitiveArrayCritical(vectorArray, vectorArrayValues,
                JNI_ABORT);

        sizes[buffered + nSerialized] = vectorSize;
        offsets[buffered + nSerialized] = offset;
    }

    jenv->ReleasePrimitiveArrayCritical(vectorSizes, vectorSizesPtr, JNI_ABORT);

    EXIT_TRACE("serializeStridedDenseVectorsToNativeBuffer");
    return nSerialized;
}

JNI_JAVA(jint, OpenCLBridge, serializeStridedSparseVectorsToNativeBuffer)
        (JNIEnv *jenv, jclass clazz, jlong valuesBuffer, jlong indicesBuffer, jint bufferPosition,
         jlong bufferCapacity, jlong sizesBuffer, jlong offsetsBuffer,
         jint buffered, jint vectorCapacity, jobjectArray vectors,
         jintArray vectorSizes, jint nToSerialize, jint tiling) {
    ENTER_TRACE("serializeStridedSparseVectorsToNativeBuffer");
    jenv->EnsureLocalCapacity(nToSerialize);

    double *serializedValues = (double *)valuesBuffer;
    int *serializedIndices = (int *)indicesBuffer;
    int *sizes = (int *)sizesBuffer;
    int *offsets = (int *)offsetsBuffer;

    int *vectorSizesPtr = (int *)jenv->GetPrimitiveArrayCritical(vectorSizes, NULL);
    CHECK_JNI(vectorSizesPtr);

    for (int i = 0; i < nToSerialize; i++) {
        const long offset = bufferPosition + i;
        jobject vector = jenv->GetObjectArrayElement(vectors, i);
        CHECK_JNI(vector);

        const jint vectorSize = vectorSizesPtr[i];
        const long lastElement = offset + ((vectorSize - 1) * tiling);
        if (buffered + i >= vectorCapacity || lastElement >= bufferCapacity) {
            EXIT_TRACE("serializeStridedSparseVectorsToNativeBuffer");
            return i;
        }

        // double[] array backing the sparse vector
        jarray vectorValues = (jarray)jenv->CallObjectMethod(vector,
                sparseVectorValuesMethod);
        CHECK_JNI(vectorValues);
        jarray vectorIndices = (jarray)jenv->CallObjectMethod(vector,
                sparseVectorIndicesMethod);
        CHECK_JNI(vectorIndices);

        double *vectorArrayValues = (double *)jenv->GetPrimitiveArrayCritical(
                vectorValues, NULL);
        CHECK_JNI(vectorArrayValues);
        int *vectorArrayIndices = (int *)jenv->GetPrimitiveArrayCritical(
                vectorIndices, NULL);
        CHECK_JNI(vectorArrayIndices);

        for (int j = 0; j < vectorSize; j++) {
            serializedValues[offset + (j * tiling)] = vectorArrayValues[j];
            serializedIndices[offset + (j * tiling)] = vectorArrayIndices[j];
        }

        jenv->ReleasePrimitiveArrayCritical(vectorValues, vectorArrayValues,
                JNI_ABORT);
        jenv->ReleasePrimitiveArrayCritical(vectorIndices, vectorArrayIndices,
                JNI_ABORT);

        sizes[buffered + i] = vectorSize;
        offsets[buffered + i] = offset;
    }

    jenv->ReleasePrimitiveArrayCritical(vectorSizes, vectorSizesPtr, JNI_ABORT);

    EXIT_TRACE("serializeStridedSparseVectorsToNativeBuffer");
    return nToSerialize;
}

JNI_JAVA(void, OpenCLBridge, storeNLoaded)(JNIEnv *jenv, jclass clazz, jint rddid,
         jint partitionid, jint offsetid, jint n_loaded) {
    assert(rddid >= 0);

#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int err = pthread_rwlock_wrlock(&nloaded_cache_lock);
    ASSERT(err == 0);
#ifdef PROFILE_LOCKS
    __sync_fetch_and_add(&nloaded_cache_lock_contention, get_clock_gettime_ns() -
            start);
#endif

    rdd_partition_offset uuid(rddid, partitionid, offsetid, 0);
    map<rdd_partition_offset, int>::iterator found = nloaded_cache->find(uuid);
    if (found != nloaded_cache->end()) {
#ifdef VERBOSE
        fprintf(stderr, "Checking that existing nloaded=%d is the same as new "
                "nloaded=%d for rdd=%d partition=%d offset=%d\n", found->second,
                n_loaded, rddid, partitionid, offsetid);
#endif
        assert(found->second == n_loaded);
    } else {
#ifdef VERBOSE
        fprintf(stderr, "Setting new nloaded for rdd=%d partition=%d offset=%d "
                "to nloaded=%d\n", rddid, partitionid, offsetid, n_loaded);
#endif
        bool success = nloaded_cache->insert(pair<rdd_partition_offset, int>(
                    uuid, n_loaded)).second;
        assert(success);
    }

    err = pthread_rwlock_unlock(&nloaded_cache_lock);
    ASSERT(err == 0);
}

JNI_JAVA(jint, OpenCLBridge, fetchNLoaded)(JNIEnv *jenv, jclass clazz, jint rddid,
         jint partitionid, jint offsetid) {
#ifdef VERBOSE
    fprintf(stderr, "fetchNLoaded: rddid=%d partitionid=%d offsetid=%d\n",
            rddid, partitionid, offsetid);
#endif
    assert(rddid >= 0);
    int result;

#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int err = pthread_rwlock_rdlock(&nloaded_cache_lock);
#ifdef PROFILE_LOCKS
    __sync_fetch_and_add(&nloaded_cache_lock_contention, get_clock_gettime_ns() -
            start);
#endif
    ASSERT(err == 0);

    rdd_partition_offset uuid(rddid, partitionid, offsetid, 0);
    map<rdd_partition_offset, int>::iterator found = nloaded_cache->find(uuid);
    if (found != nloaded_cache->end()) {
        result = found->second;
    } else {
        result = -1;
    }

    err = pthread_rwlock_unlock(&nloaded_cache_lock);
    ASSERT(err == 0);

#ifdef VERBOSE
    fprintf(stderr, "fetchNLoaded: for rddid=%d partitionid=%d offsetid=%d "
            "returning result=%d\n", rddid, partitionid, offsetid, result);
#endif

    return result;
}

JNI_JAVA(void, OpenCLBridge, copyNativeArrayToJVMArray)(JNIEnv *jenv,
        jclass clazz, jlong buffer, jint offset, jobject arr, jint size) {
    ENTER_TRACE("copyNativeArrayToByteArray");

    char *src = (char *)buffer;
    char *dst = (char *)jenv->GetPrimitiveArrayCritical((jarray)arr, NULL);
    memcpy(dst, src + offset, size);
    jenv->ReleasePrimitiveArrayCritical((jarray)arr, dst, JNI_ABORT);

    EXIT_TRACE("copyNativeArrayToByteArray");
}

JNI_JAVA(void, OpenCLBridge, copyJVMArrayToNativeArray)(JNIEnv *jenv,
        jclass clazz, jlong buffer, jint bufferOffset, jobject arr,
        jint arrOffset, jint size) {
    ENTER_TRACE("copyByteArrayToNativeArray");

    char *dst = (char *)buffer;
    char *src = (char *)jenv->GetPrimitiveArrayCritical((jarray)arr, NULL);
    memcpy(dst + bufferOffset, src + arrOffset, size);
    jenv->ReleasePrimitiveArrayCritical((jarray)arr, src, JNI_ABORT);

    EXIT_TRACE("copyByteArrayToNativeArray");
}

typedef struct _callback_data {
    swat_context *ctx;
    int buffer_id;
} callback_data;

static void add_freed_native_buffer(swat_context *ctx, int buffer_id,
        cl_event event) {
    native_input_buffer_list_node *freed =
        (native_input_buffer_list_node *)malloc(
                sizeof(native_input_buffer_list_node));
    CHECK_ALLOC(freed);

    freed->id = buffer_id;
    freed->event = event;
    freed->next = NULL;

#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int perr = pthread_mutex_lock(&ctx->freed_native_input_buffers_lock);
#ifdef PROFILE_LOCKS
    ctx->freed_native_input_buffers_lock_contention += (get_clock_gettime_ns() -
            start);
#endif
    ASSERT(perr == 0);

    if (ctx->freed_native_input_buffers == NULL) {
        ctx->freed_native_input_buffers = freed;
    } else {
        native_input_buffer_list_node *curr = ctx->freed_native_input_buffers;
        while (curr->next != NULL) {
            curr = curr->next;
        }
        curr->next = freed;
    }

    perr = pthread_cond_signal(&ctx->freed_native_input_buffers_cond);
    ASSERT(perr == 0);

    perr = pthread_mutex_unlock(&ctx->freed_native_input_buffers_lock);
    ASSERT(perr == 0);
}

JNI_JAVA(jint, OpenCLBridge, waitForFreedNativeBuffer)(JNIEnv *jenv,
        jclass clazz, jlong l_ctx, jlong l_dev_ctx) {
    ENTER_TRACE("waitForFreedNativeBuffer");
    swat_context *ctx = (swat_context *)l_ctx;

#ifdef PROFILE_LOCKS
    const unsigned long long start = get_clock_gettime_ns();
#endif
    int perr = pthread_mutex_lock(&ctx->freed_native_input_buffers_lock);
#ifdef PROFILE_LOCKS
    ctx->freed_native_input_buffers_lock_contention += (get_clock_gettime_ns() -
            start);
#endif
    ASSERT(perr == 0);

    while (ctx->freed_native_input_buffers == NULL) {
        int perr = pthread_cond_wait(&ctx->freed_native_input_buffers_cond,
                &ctx->freed_native_input_buffers_lock);
        ASSERT(perr == 0);
    }
    native_input_buffer_list_node *released = ctx->freed_native_input_buffers;
    ctx->freed_native_input_buffers = ctx->freed_native_input_buffers->next;

#ifdef PROFILE_LOCKS
    ctx->freed_native_input_buffers_blocked += (get_clock_gettime_ns() - start);
#endif
    perr = pthread_mutex_unlock(&ctx->freed_native_input_buffers_lock);
    ASSERT(perr == 0);

    int buffer_id = released->id;
    if (released->event) {
        CHECK(clWaitForEvents(1, &released->event));
    }
    free(released);

    EXIT_TRACE("waitForFreedNativeBuffer");
    return buffer_id;
}

JNI_JAVA(void, OpenCLBridge, addFreedNativeBuffer)(JNIEnv *jenv,
        jclass clazz, jlong l_ctx, jlong l_dev_ctx, jint buffer_id) {
    ENTER_TRACE("addFreedNativeBuffer");
    swat_context *ctx = (swat_context *)l_ctx;

    add_freed_native_buffer(ctx, buffer_id, NULL);

    EXIT_TRACE("addFreedNativeBuffer");
}

JNI_JAVA(void, OpenCLBridge, enqueueBufferFreeCallback)(JNIEnv *jenv,
        jclass clazz, jlong l_ctx, jlong l_dev_ctx, jint buffer_id) {
    ENTER_TRACE("enqueueBufferFreeCallback");
    swat_context *ctx = (swat_context *)l_ctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;

    ASSERT(ctx->last_write_event);
    add_freed_native_buffer(ctx, buffer_id, ctx->last_write_event);

    EXIT_TRACE("enqueueBufferFreeCallback");
}

JNI_JAVA(jint, OpenCLBridge, getCurrentSeqNo)(JNIEnv *jenv, jclass clazz,
        jlong l_ctx) {
    swat_context *ctx = (swat_context *)l_ctx;
    return ctx->run_seq_no;
}

JNI_JAVA(void, OpenCLBridge, pinnedToJVMArray)(JNIEnv *jenv, jclass clazz,
        jlong l_kernel_ctx, jobject primitive_arr, jlong l_pinned, jint nbytes) {
    kernel_context *kernel_ctx = (kernel_context *)l_kernel_ctx;
    void *pinned = (void *)l_pinned;
#ifdef VERBOSE
    fprintf(stderr, "pinnedToJVMArray: thread=%d pinned=%p nbytes=%d\n",
            kernel_ctx->ctx->host_thread_index, pinned, nbytes);
#endif

    void *jvm_arr = jenv->GetPrimitiveArrayCritical((jarray)primitive_arr, NULL);
    ASSERT(jvm_arr);
    memcpy(jvm_arr, pinned, nbytes);
    jenv->ReleasePrimitiveArrayCritical((jarray)primitive_arr, jvm_arr, 0);
}

JNI_JAVA(void, OpenCLBridge, fillHeapBuffersFromKernelContext)(JNIEnv *jenv,
        jclass clazz, jlong l_kernel_ctx, jlongArray jvmArr, jint maxHeaps) {
    kernel_context *kernel_ctx = (kernel_context *)l_kernel_ctx;
    ASSERT(kernel_ctx->n_heap_ctxs <= maxHeaps);

    jlong *jvm = (jlong *)jenv->GetPrimitiveArrayCritical(jvmArr, NULL);
    ASSERT(jvm);

    int i = 0;
    for ( ; i < kernel_ctx->n_heap_ctxs; i++) {
        jvm[i] = (jlong)((kernel_ctx->heaps)[i].heap_ctx->pinned_h_heap);
    }
    for ( ; i < maxHeaps; i++) {
        jvm[i] = 0L;
    }
    jenv->ReleasePrimitiveArrayCritical(jvmArr, jvm, 0);
}

JNI_JAVA(jint, OpenCLBridge, getNLoaded)(JNIEnv *jenv, jclass clazz,
        jlong l_kernel_ctx) {
    kernel_context *kernel_ctx = (kernel_context *)l_kernel_ctx;
    return kernel_ctx->n_loaded;
}

JNI_JAVA(void, OpenCLBridge, waitOnBufferReady)(JNIEnv *jenv, jclass clazz,
        jlong l_kernel_complete) {
    kernel_complete_flag *kernel_complete = (kernel_complete_flag *)l_kernel_complete;

    int perr = pthread_mutex_lock(&kernel_complete->lock);
    ASSERT(perr == 0);

    while (kernel_complete->done == 0) {
        perr = pthread_cond_wait(&kernel_complete->cond, &kernel_complete->lock);
        ASSERT(perr == 0);
    }

    perr = pthread_mutex_unlock(&kernel_complete->lock);
    ASSERT(perr == 0);

    free(kernel_complete);
}

JNI_JAVA(jlong, OpenCLBridge, clMallocImpl)(JNIEnv *jenv, jclass clazz,
        jlong l_dev_ctx, jlong nbytes) {
    device_context *dev_ctx = (device_context *)l_dev_ctx;
#ifdef VERBOSE
    fprintf(stderr, "clMallocImpl: nbytes=%ld on device %d\n", nbytes,
            dev_ctx->device_index);
#endif
    cl_region *region = allocate_cl_region(nbytes, dev_ctx->allocator, NULL, NULL);
#ifdef VERBOSE
    fprintf(stderr, "clMallocImpl: return region=%p for nbytes=%ld on device "
            "%d\n", region, nbytes, dev_ctx->device_index);
#endif
    return (jlong)region;
}

JNI_JAVA(void, OpenCLBridge, clFree)(JNIEnv *jenv, jclass clazz, jlong l_region,
        jlong l_dev_ctx) {
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_region *region = (cl_region *)l_region;
#ifdef VERBOSE
    fprintf(stderr, "clFree: region=%p\n", region);
#endif

    free_cl_region(region, false);
}

JNI_JAVA(jlong, OpenCLBridge, pin)(JNIEnv *jenv, jclass clazz,
        jlong l_dev_ctx, jlong l_region) {
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_region *region = (cl_region *)l_region;
#ifdef VERBOSE
    fprintf(stderr, "pin: region=%p\n", region);
#endif

    void *pinned = fetch_pinned(region);

#ifdef VERBOSE
    fprintf(stderr, "pin: for region=%p returning pinned=%p\n", region,
            pinned);
#endif

    return (jlong)pinned;
}

JNI_JAVA(void, OpenCLBridge, setNativePinnedArrayArg)(JNIEnv *jenv,
        jclass clazz, jlong lctx, jlong ldev_ctx, jint index,
        jlong pinned_buffer, jlong l_region, jlong nbytes) {
    ENTER_TRACE("setNativePinnedArrayArg");
#ifdef VERBOSE
    fprintf(stderr, "setNativePinnedArrayArg: index=%d pinned=%p region=%p "
            "nbytes=%ld\n", index, (void *)pinned_buffer, (void *)l_region,
            nbytes);
#endif

    device_context *dev_ctx = (device_context *)ldev_ctx;
    swat_context *context = (swat_context *)lctx;

    void *pinned = (void *)pinned_buffer;
    cl_region *region = (cl_region *)l_region;

    add_pending_region_arg(context, index, false, true, false, region);

    cl_event event;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem,
                CL_FALSE, 0, nbytes, pinned, context->last_write_event ? 1 : 0,
                context->last_write_event ? &context->last_write_event : NULL, &event));
#ifdef PROFILE_OPENCL
    add_event_to_list(&context->acc_write_events, event, "init_write",
            &context->acc_write_events_length,
            &context->acc_write_events_capacity);
#endif
    context->last_write_event = event;

    EXIT_TRACE("setNativePinnedArrayArg");
}

JNI_JAVA(void, OpenCLBridge, setOutArrayArg)(JNIEnv *jenv, jclass clazz,
        jlong lctx, jlong ldev_ctx, jint index, jlong l_region) {
    device_context *dev_ctx = (device_context *)ldev_ctx;
    swat_context *ctx = (swat_context *)lctx;
    cl_region *region = (cl_region *)l_region;

    add_pending_region_arg(ctx, index, false, true, true, region);
}

JNI_JAVA(jint, OpenCLBridge, getOutputBufferIdFromKernelCtx)(JNIEnv *jenv,
        jclass clazz, jlong lkernel_ctx) {
    kernel_context *kernel_ctx = (kernel_context *)lkernel_ctx;
    ASSERT(kernel_ctx->output_buffer_id >= 0);
    return kernel_ctx->output_buffer_id;
}

#ifdef __cplusplus
}
#endif
