#include <jni.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "kernel_arg.h"
#include "bridge.h"
#include "common.h"
#include "ocl_util.h"

#ifdef VERBOSE
#define TRACE(...) fprintf(stderr, __VA_ARGS__)
#else
#define TRACE(...)
#endif

static device_context *device_ctxs = NULL;
static int n_device_ctxs = 0;
/*
 * Only used when initializing device contexts to ensure only one thread
 * initializes them. Should be very little contention on this lock.
 */
static pthread_mutex_t device_ctxs_lock = PTHREAD_MUTEX_INITIALIZER;
static int *virtual_devices = NULL;
static int n_virtual_devices = 0;

/*
 * Inter-device RDD cache
 */
static map<rdd_partition_offset, map<int, cl_region *> *> *rdd_cache;

/*
 * Cache a mapping from unique RDD identifier to the number of items loaded for
 * that part of the RDD.
 */
static map<rdd_partition_offset, int> *nloaded_cache;

/*
 * Cache certain JNI things.
 */
jclass denseVectorClass;
jmethodID denseVectorSizeMethod;
jmethodID denseVectorValuesMethod;

/*
 * Read lock is acquired when looking for a hint for which device to run on.
 * Read lock is also acquired when checking if there's a cached version of a
 * piece of an RDD. Write lock is acquired when we need to update metadata on
 * the RDD caching.
 */
static pthread_rwlock_t rdd_cache_lock = PTHREAD_RWLOCK_INITIALIZER;

#define ARG_MACRO(ltype, utype) \
JNI_JAVA(void, OpenCLBridge, set##utype##Arg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jint index, j##ltype arg) { \
    ENTER_TRACE("set"#utype"Arg"); \
    swat_context *context = (swat_context *)lctx; \
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(arg), &arg); \
    EXIT_TRACE("set"#utype"Arg"); \
}

#define SET_ARRAY_ARG_MACRO(ltype, utype, ctype) \
JNI_JAVA(jboolean, OpenCLBridge, set##utype##ArrayArgImpl) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, \
         j##ltype##Array arg, jint argLength, jlong broadcastId, jint rddid, \
         jint partitionid, jint offsetid, jint componentid) { \
    ENTER_TRACE("set"#utype"ArrayArg"); \
    jsize len = argLength * sizeof(ctype); \
    device_context *dev_ctx = (device_context *)l_dev_ctx; \
    swat_context *context = (swat_context *)lctx; \
    jboolean isCopy; \
    if (broadcastId >= 0) { \
        ASSERT_MSG(rddid < 0 && componentid >= 0, "broadcast check"); \
        broadcast_id uuid(broadcastId, componentid); \
        int err = pthread_rwlock_rdlock(&dev_ctx->broadcast_lock); \
        ASSERT(err == 0); \
        \
        cl_region *region = NULL; \
        map<broadcast_id, cl_region *>::iterator found = dev_ctx->broadcast_cache->find(uuid); \
        if (found != dev_ctx->broadcast_cache->end()) { \
            region = found->second; \
        } \
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index)); \
        err = pthread_rwlock_unlock(&dev_ctx->broadcast_lock); \
        ASSERT(err == 0); \
        if (reallocated) { \
            TRACE("caching broadcast %d %d\n", broadcastId, componentid); \
            set_cached_kernel_arg(region, index, len, context, dev_ctx); \
            (*context->arguments)[index] = pair<cl_region *, bool>(region, true); \
        } else { \
            void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
            CHECK_JNI(arr) \
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index, context, \
                    dev_ctx, broadcastId, rddid, NULL); \
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
            if (new_region == NULL) return false; \
            \
            err = pthread_rwlock_wrlock(&dev_ctx->broadcast_lock); \
            ASSERT(err == 0); \
            (*dev_ctx->broadcast_cache)[uuid] = new_region; \
            TRACE("adding broadcast %d %d to cache\n", broadcastId, \
                    componentid); \
            err = pthread_rwlock_unlock(&dev_ctx->broadcast_lock); \
            ASSERT(err == 0); \
        } \
    } else if (rddid >= 0) { \
        ASSERT_MSG(broadcastId < 0 && partitionid >= 0 && offsetid >= 0 && componentid >= 0, "check RDD"); \
        rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid); \
        cl_region *region = NULL; \
        int err = pthread_rwlock_rdlock(&rdd_cache_lock); \
        ASSERT(err == 0); \
        map<rdd_partition_offset, map<int, cl_region *> *>::iterator found = rdd_cache->find(uuid); \
        if (found != rdd_cache->end()) { \
            map<int, cl_region *> *cached = found->second; \
            map<int, cl_region *>::iterator found_in_cache = cached->find(dev_ctx->device_index); \
            if (found_in_cache != cached->end()) { \
                region = found_in_cache->second; \
            } \
        } \
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index)); \
        err = pthread_rwlock_unlock(&rdd_cache_lock); \
        ASSERT(err == 0); \
        if (reallocated) { \
            TRACE("caching rdd=%d partition=%d offset=%d component=%d\n", \
                    rddid, partitionid, offsetid, componentid); \
            set_cached_kernel_arg(region, index, len, context, dev_ctx); \
            (*context->arguments)[index] = pair<cl_region *, bool>(region, true); \
        } else { \
            void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index, context, \
                    dev_ctx, broadcastId, rddid, NULL); \
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
            if (new_region == NULL) return false; \
            \
            err = pthread_rwlock_wrlock(&rdd_cache_lock); \
            ASSERT(err == 0); \
            if (rdd_cache->find(uuid) == rdd_cache->end()) { \
                bool success = rdd_cache->insert( \
                        pair<rdd_partition_offset, map<int, cl_region *> *>( \
                            uuid, new map<int, cl_region *>())).second; \
                ASSERT_MSG(success, "uuid insert"); \
            } \
            (*rdd_cache->at(uuid))[dev_ctx->device_index] = new_region; \
            TRACE("adding rdd=%d partition=%d offset=%d component=%d\n", \
                    rddid, partitionid, offsetid, componentid); \
            err = pthread_rwlock_unlock(&rdd_cache_lock); \
            ASSERT(err == 0); \
        } \
    } else { \
        ASSERT_MSG(rddid < 0 && broadcastId < 0, "neither RDD or broadcast"); \
        void *arr = jenv->GetPrimitiveArrayCritical(arg, &isCopy); \
        CHECK_JNI(arr) \
        cl_region *new_region = set_and_write_kernel_arg(arr, len, index, context, dev_ctx, \
                broadcastId, rddid, NULL); \
        jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT); \
        if (new_region == NULL) return false; \
        \
    } \
    EXIT_TRACE("set"#utype"ArrayArg"); \
    return true; \
}

#define FETCH_ARRAY_ARG_MACRO(ltype, utype, ctype) \
JNI_JAVA(void, OpenCLBridge, fetch##utype##ArrayArg) \
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index, \
         j##ltype##Array arg, jint argLength, jlong broadcastId) { \
    ENTER_TRACE("fetch"#utype"ArrayArg"); \
    jsize len = argLength * sizeof(ctype); \
    void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL); \
    CHECK_JNI(arr) \
    fetch_kernel_arg(arr, len, index, (swat_context *)lctx, (device_context *)l_dev_ctx); \
    jenv->ReleasePrimitiveArrayCritical(arg, arr, 0); \
    EXIT_TRACE("fetch"#utype"ArrayArg"); \
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
    clSetKernelArgWrapper(context, context->kernel, index, sizeof(val), &val); \
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

static void add_event_to_context(swat_context *context, cl_event event) {
    assert(context->n_events <= context->event_capacity);
    if (context->n_events == context->event_capacity) {
        context->event_capacity *= 2;
        context->events = (cl_event *)realloc(context->events,
                context->event_capacity * sizeof(cl_event));
        CHECK_ALLOC(context->events);
    }
    (context->events)[context->n_events] = event;
    context->n_events = context->n_events + 1;
}

static void clSetKernelArgWrapper(swat_context *context, cl_kernel kernel,
        cl_uint arg_index, size_t arg_size, void *arg_value) {
#ifdef VERBOSE
    fprintf(stderr, "setting arg %u to value %d with size %lu\n", arg_index,
            *((int *)arg_value), arg_size);
#endif

    CHECK(clSetKernelArg(kernel, arg_index, arg_size, arg_value));
#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[arg_index] = new kernel_arg(arg_value, arg_size,
            false, false);
#endif

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
        CHECK_ALLOC(exts)
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

static void populateDeviceContexts(JNIEnv *jenv) {
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
    int perr = pthread_mutex_lock(&device_ctxs_lock);
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
                fprintf(stderr, "SWAT %d: platform %d, device %d, %s (%s)\n",
                        global_device_id, platform_index, i, device_name,
                        get_device_type_str(curr_dev));
                free(device_name);
#endif

                cl_int err;
                cl_context_properties ctx_props[] = { CL_CONTEXT_PLATFORM,
                    (cl_context_properties)platforms[platform_index], 0 };
                cl_context ctx = clCreateContext(ctx_props, 1, &curr_dev, NULL, NULL, &err);
                CHECK(err);

                cl_command_queue_properties props = 0;
#ifndef __APPLE__
                props = CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE;
#endif

                cl_command_queue cmd = clCreateCommandQueue(ctx, curr_dev,
                        props, &err);
                CHECK(err);

                tmp_device_ctxs[global_device_id].platform = platforms[platform_index];
                tmp_device_ctxs[global_device_id].dev = curr_dev;
                tmp_device_ctxs[global_device_id].ctx = ctx;
                tmp_device_ctxs[global_device_id].cmd = cmd;
                tmp_device_ctxs[global_device_id].device_index = global_device_id;

                int perr = pthread_rwlock_init(
                        &(tmp_device_ctxs[global_device_id].broadcast_lock),
                        NULL);
                ASSERT(perr == 0);
                perr = pthread_mutex_init(
                        &(tmp_device_ctxs[global_device_id].program_cache_lock),
                        NULL);
                ASSERT(perr == 0);

                tmp_device_ctxs[global_device_id].allocator = init_allocator(
                        curr_dev, global_device_id, ctx, cmd);

                tmp_device_ctxs[global_device_id].broadcast_cache =
                    new map<broadcast_id, cl_region *>();
                tmp_device_ctxs[global_device_id].program_cache =
                    new map<string, cl_program>();

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

        rdd_cache = new map<rdd_partition_offset, map<int, cl_region *> *>();
        nloaded_cache = new map<rdd_partition_offset, int>();

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

        denseVectorClass = jenv->FindClass(
                "org/apache/spark/mllib/linalg/DenseVector");
        CHECK_JNI(denseVectorClass);
        denseVectorSizeMethod = jenv->GetMethodID(denseVectorClass, "size",
                "()I");
        CHECK_JNI(denseVectorSizeMethod);
        denseVectorValuesMethod = jenv->GetMethodID(denseVectorClass, "values",
                "()[D");
        CHECK_JNI(denseVectorValuesMethod);

        device_ctxs = tmp_device_ctxs;
    }

    perr = pthread_mutex_unlock(&device_ctxs_lock);
    ASSERT(perr == 0);
}

JNI_JAVA(jint, OpenCLBridge, getDeviceToUse)
        (JNIEnv *jenv, jclass clazz, jint hint, jint host_thread_index) {
    ENTER_TRACE("getDeviceToUse");
    populateDeviceContexts(jenv);

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

    int perr = pthread_rwlock_rdlock(&rdd_cache_lock);
    ASSERT(perr == 0);

    if (rdd_cache) {
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

    perr = pthread_rwlock_unlock(&rdd_cache_lock);
    ASSERT(perr == 0);

    EXIT_TRACE("getDeviceHintFor");
    return result;
}

JNI_JAVA(jlong, OpenCLBridge, getActualDeviceContext)
        (JNIEnv *jenv, jclass clazz, int device_index) {

    populateDeviceContexts(jenv);

    ASSERT(device_index < n_device_ctxs);
    return (jlong)(device_ctxs + device_index);
}

JNI_JAVA(void, OpenCLBridge, cleanupSwatContext)
        (JNIEnv *jenv, jclass clazz, jlong l_ctx) {
    swat_context *ctx = (swat_context *)l_ctx;
    CHECK(clReleaseKernel(ctx->kernel));
    delete ctx->arguments;
    free(ctx);
}

JNI_JAVA(jlong, OpenCLBridge, createSwatContext)
        (JNIEnv *jenv, jclass clazz, jstring label, jstring source,
         jlong l_dev_ctx, jint host_thread_index, jboolean requiresDouble,
         jboolean requiresHeap) {
    ENTER_TRACE("createContext");
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_device_id device = dev_ctx->dev;

    ASSERT(checkAllAssertions(device, requiresDouble, requiresHeap) == 1);

#ifdef VERBOSE
    char *device_name = get_device_name(device);

    fprintf(stderr, "SWAT: Using device %s, require double? %d, "
            "require heap? %d\n", device_name, requiresDouble,
            requiresHeap);
    free(device_name);
#endif
    const char *raw_label = jenv->GetStringUTFChars(label, NULL);
    CHECK_JNI(raw_label)
    std::string label_str(raw_label);
    jenv->ReleaseStringUTFChars(label, raw_label);

    int perr = pthread_mutex_lock(&dev_ctx->program_cache_lock);
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
        CHECK_ALLOC(store_source)
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
        CHECK_ALLOC(store_source)
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
            CHECK_ALLOC(build_log)
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
    CHECK_ALLOC(context)
    context->kernel = kernel;
    context->host_thread_index = host_thread_index;
    context->arguments = new map<int, pair<cl_region *, bool> >();
    context->event_capacity = 10;
    context->n_events = 0;
    context->events = (cl_event *)malloc(context->event_capacity *
            sizeof(cl_event));
#ifdef BRIDGE_DEBUG
    context->debug_arguments = new map<int, kernel_arg *>();
    context->kernel_src = store_source;
    context->kernel_src_len = source_len;
#endif
    EXIT_TRACE("createContext");
    return (jlong)context;
}

static void postKernelCleanupHelper(swat_context *ctx) {

    // Not normally necessary, but necessary for our testing without doing kernel launch
    CHECK(clWaitForEvents(ctx->n_events, ctx->events));
    ctx->n_events = 0;

    cl_allocator *allocator = NULL;
    for (map<int, pair<cl_region *, bool> >::iterator i = ctx->arguments->begin(),
            e = ctx->arguments->end(); i != e; i++) {
        cl_region *region = i->second.first;
        if (region) {
            if (allocator) {
                ASSERT(allocator == region->grandparent->allocator);
            } else {
                allocator = region->grandparent->allocator;
            }
#ifdef VERBOSE
            fprintf(stderr, "Freeing index=%d try_to_keep=%d region=%p\n",
                    i->first, i->second.second, region);
#endif
            free_cl_region(region, i->second.second);
        }
    }

    if (allocator) {
        bump_time(allocator);
    }
}

static void postKernelCleanupHelperWrapper(void *ctx) {
    postKernelCleanupHelper((swat_context *)ctx);
}

static cl_region *get_mem_cached(swat_context *context, device_context *dev_ctx,
        int index, size_t size, jlong broadcastId, jint rdd) {
#ifdef VERBOSE
    fprintf(stderr, "%d: Allocating region of size %lu bytes for index=%d\n",
            context->host_thread_index, size, index);
#endif

    cl_region *region = allocate_cl_region(size, dev_ctx->allocator,
            postKernelCleanupHelperWrapper, context);
    if (region == NULL) return NULL;

#ifdef VERBOSE
    fprintf(stderr, "%d: Got %p for %lu bytes for index=%d\n",
            context->host_thread_index, region, size, index);
#endif

    (*context->arguments)[index] = pair<cl_region *, bool>(region,
            broadcastId >= 0 || rdd >= 0);
    return region;
}

static void set_cached_kernel_arg(cl_region *region, int index, size_t len,
        swat_context *context, device_context *dev_ctx) {
    CHECK(clSetKernelArg(context->kernel, index, sizeof(region->sub_mem), &region->sub_mem));

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[index] = new kernel_arg(region->sub_mem, len, dev_ctx);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a memory buffer of size %lu\n", index,
            len);
#endif
}

static cl_region *set_and_write_kernel_arg(void *host, size_t len, int index,
        swat_context *context, device_context *dev_ctx, jlong broadcastId,
        jint rdd, cl_event *event) {
    cl_region *region = get_mem_cached(context, dev_ctx, index, len, broadcastId, rdd);
    if (region == NULL) return NULL;

    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem,
                event == NULL ? CL_TRUE : CL_FALSE, 0, len, host, 0, NULL,
                event));

    CHECK(clSetKernelArg(context->kernel, index, sizeof(region->sub_mem), &region->sub_mem));

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[index] = new kernel_arg(host, len, true, false);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a memory buffer of size %lu\n", index,
            len);
#endif
    return region;
}

static void fetch_kernel_arg(void *host, size_t len, int index,
        swat_context *context, device_context *dev_ctx) {
    ASSERT(context->arguments->find(index) != context->arguments->end());
    cl_mem mem = context->arguments->at(index).first->sub_mem;
#ifdef VERBOSE
    fprintf(stderr, "Fetching mem=%p to host=%p at index=%d with size=%lu\n",
            mem, host, index, len);
#endif

    CHECK(clEnqueueReadBuffer(dev_ctx->cmd, mem, CL_TRUE, 0, len, host,
                0, NULL, NULL));
}

ARG_MACRO(int, Int)

SET_PRIMITIVE_ARG_BY_NAME_MACRO(int, Int, "I")
SET_PRIMITIVE_ARG_BY_NAME_MACRO(double, Double, "D")
SET_PRIMITIVE_ARG_BY_NAME_MACRO(float, Float, "F")

SET_ARRAY_ARG_MACRO(int, Int, int)
SET_ARRAY_ARG_MACRO(double, Double, double)
SET_ARRAY_ARG_MACRO(float, Float, float)
SET_ARRAY_ARG_MACRO(byte, Byte, jbyte)

FETCH_ARRAY_ARG_MACRO(int, Int, int)
FETCH_ARRAY_ARG_MACRO(double, Double, double)
FETCH_ARRAY_ARG_MACRO(float, Float, float)
FETCH_ARRAY_ARG_MACRO(byte, Byte, jbyte)

JNI_JAVA(int, OpenCLBridge, getDevicePointerSizeInBytes)
        (JNIEnv *jenv, jclass clazz, jlong l_dev_ctx) {
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_uint pointer_size_in_bits;
    CHECK(clGetDeviceInfo(dev_ctx->dev, CL_DEVICE_ADDRESS_BITS,
                sizeof(pointer_size_in_bits), &pointer_size_in_bits, NULL));
    assert(pointer_size_in_bits % 8 == 0);
    return pointer_size_in_bits / 8;
}

static cl_region *is_cached(jlong broadcastId, jint rddid, jint partitionid,
        jint offsetid, jint componentid, device_context *dev_ctx,
        swat_context *context) {
    cl_region *region = NULL;

    if (broadcastId >= 0) {
        ASSERT(rddid < 0 && componentid >= 0);
        broadcast_id uuid(broadcastId, componentid);
        int err = pthread_rwlock_rdlock(&dev_ctx->broadcast_lock);
        ASSERT(err == 0);

        map<broadcast_id, cl_region *>::iterator found = dev_ctx->broadcast_cache->find(uuid);
        if (found != dev_ctx->broadcast_cache->end()) {
            region = found->second;
        }
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index));
        err = pthread_rwlock_unlock(&dev_ctx->broadcast_lock);
        ASSERT(err == 0);

        if (!reallocated) region = NULL;
    } else if (rddid >= 0) {
        ASSERT(broadcastId < 0 && partitionid >= 0 && offsetid >= 0 && componentid >= 0); \
        rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid);

        int err = pthread_rwlock_rdlock(&rdd_cache_lock);
        ASSERT(err == 0);
        map<rdd_partition_offset, map<int, cl_region *> *>::iterator found =
            rdd_cache->find(uuid);
        if (found != rdd_cache->end()) {
            map<int, cl_region *> *cached = found->second;
            map<int, cl_region *>::iterator found_in_cache = cached->find(dev_ctx->device_index);
            if (found_in_cache != cached->end()) {
                region = found_in_cache->second;
            }
        }
        bool reallocated = (region && re_allocate_cl_region(region, dev_ctx->device_index));
        err = pthread_rwlock_unlock(&rdd_cache_lock);
        ASSERT(err == 0);

        if (!reallocated) region = NULL;
    }

    return region;
}

JNI_JAVA(void, OpenCLBridge, fillFromNativeArray)(JNIEnv *jenv, jclass clazz,
            jarray vectorArr, jint vectorSize, jint vectorOffset, jint tiling,
            jlong buffer) {
    ENTER_TRACE("fillFromNativeArray");
    double *nativeBuffer = (double *)buffer;

    double *arr = (double *)jenv->GetPrimitiveArrayCritical(vectorArr, NULL);
    CHECK_JNI(arr);
    for (int i = 0; i < vectorSize; i++) {
        arr[i] = nativeBuffer[vectorOffset + (i * tiling)];
    }
    jenv->ReleasePrimitiveArrayCritical(vectorArr, arr, JNI_COMMIT);
    EXIT_TRACE("fillFromNativeArray");
}

JNI_JAVA(jboolean, OpenCLBridge, setNativeArrayArgImpl)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index,
         jlong buffer, jint len, jlong broadcastId, jint rddid,
         jint partitionid, jint offsetid, jint componentid) {
    ENTER_TRACE("setNativeArrayArg");
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    swat_context *context = (swat_context *)lctx;

    cl_region *reallocated = is_cached(broadcastId, rddid, partitionid,
            offsetid, componentid, dev_ctx, context);
    if (broadcastId >= 0) {
        if (reallocated) {
            TRACE("caching broadcast %d %d\n", broadcastId, componentid);
            set_cached_kernel_arg(reallocated, index, len, context, dev_ctx);
            (*context->arguments)[index] = pair<cl_region *, bool>(reallocated, true);
        } else {
            broadcast_id uuid(broadcastId, componentid);
            void *arr = (void *)buffer;
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                    context, dev_ctx, broadcastId, rddid, NULL);
            if (new_region == NULL) return false;

            int err = pthread_rwlock_wrlock(&dev_ctx->broadcast_lock);
            ASSERT(err == 0);
            (*dev_ctx->broadcast_cache)[uuid] = new_region;
            TRACE("adding broadcast %d %d to cache\n", broadcastId, componentid);
            err = pthread_rwlock_unlock(&dev_ctx->broadcast_lock);
            ASSERT(err == 0);
        }
    } else if (rddid >= 0) {
        if (reallocated) {
            TRACE("caching rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
            set_cached_kernel_arg(reallocated, index, len, context, dev_ctx);
            (*context->arguments)[index] = pair<cl_region *, bool>(reallocated, true);
        } else {
            rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid);
            void *arr = (void *)buffer;
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index, context,
                    dev_ctx, broadcastId, rddid, NULL);
            if (new_region == NULL) return false;

            int err = pthread_rwlock_wrlock(&rdd_cache_lock);
            ASSERT(err == 0);
            if (rdd_cache->find(uuid) == rdd_cache->end()) {
                bool success = rdd_cache->insert(
                        pair<rdd_partition_offset, map<int, cl_region *> *>(
                            uuid, new map<int, cl_region *>())).second;
                ASSERT(success);
            }
            (*rdd_cache->at(uuid))[dev_ctx->device_index] = new_region;
            TRACE("adding rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
            err = pthread_rwlock_unlock(&rdd_cache_lock);
            ASSERT(err == 0);
        }
    } else {
        assert(reallocated == NULL);
        ASSERT(rddid < 0 && broadcastId < 0);
        void *arr = (void *)buffer;
        cl_event event;
        cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                context, dev_ctx, broadcastId, rddid, &event);
        add_event_to_context(context, event);
        if (new_region == NULL) return false;
    }

    EXIT_TRACE("setNativeArrayArg");
    return true;
}

JNI_JAVA(jboolean, OpenCLBridge, setArrayArgImpl)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint index,
         jobject argObj, jint argLength, jint argEleSize, jlong broadcastId, jint rddid,
         jint partitionid, jint offsetid, jint componentid) {
    ENTER_TRACE("setArrayArg");
    jarray arg = (jarray)argObj;

    jsize len = argLength * argEleSize;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    swat_context *context = (swat_context *)lctx;

    cl_region *reallocated = is_cached(broadcastId, rddid, partitionid,
            offsetid, componentid, dev_ctx, context);

    if (broadcastId >= 0) {
        if (reallocated) {
            TRACE("caching broadcast %d %d\n", broadcastId, componentid);
            set_cached_kernel_arg(reallocated, index, len, context, dev_ctx);
            (*context->arguments)[index] = pair<cl_region *, bool>(reallocated, true);
        } else {
            broadcast_id uuid(broadcastId, componentid);
            void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL);
            CHECK_JNI(arr)
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                    context, dev_ctx, broadcastId, rddid, NULL);
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT);
            if (new_region == NULL) return false;

            int err = pthread_rwlock_wrlock(&dev_ctx->broadcast_lock);
            ASSERT(err == 0);
            (*dev_ctx->broadcast_cache)[uuid] = new_region;
            TRACE("adding broadcast %d %d to cache\n", broadcastId, componentid);
            err = pthread_rwlock_unlock(&dev_ctx->broadcast_lock);
            ASSERT(err == 0);
        }
    } else if (rddid >= 0) {
        if (reallocated) {
            TRACE("caching rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
            set_cached_kernel_arg(reallocated, index, len, context, dev_ctx);
            (*context->arguments)[index] = pair<cl_region *, bool>(reallocated,
                    true);
        } else {
            rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid);
            void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL);
            cl_region *new_region = set_and_write_kernel_arg(arr, len, index, context,
                    dev_ctx, broadcastId, rddid, NULL);
            jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT);
            if (new_region == NULL) return false;

            int err = pthread_rwlock_wrlock(&rdd_cache_lock);
            ASSERT(err == 0);
            if (rdd_cache->find(uuid) == rdd_cache->end()) {
                bool success = rdd_cache->insert(
                        pair<rdd_partition_offset, map<int, cl_region *> *>(
                            uuid, new map<int, cl_region *>())).second;
                ASSERT(success);
            }
            (*rdd_cache->at(uuid))[dev_ctx->device_index] = new_region;
            TRACE("adding rdd=%d partition=%d offset=%d component=%d\n", rddid,
                    partitionid, offsetid, componentid);
            err = pthread_rwlock_unlock(&rdd_cache_lock);
            ASSERT(err == 0);
        }
    } else {
        assert(reallocated == NULL);
        ASSERT(rddid < 0 && broadcastId < 0);
        void *arr = jenv->GetPrimitiveArrayCritical(arg, NULL);
        CHECK_JNI(arr)
        cl_region *new_region = set_and_write_kernel_arg(arr, len, index,
                context, dev_ctx, broadcastId, rddid, NULL);
        jenv->ReleasePrimitiveArrayCritical(arg, arr, JNI_ABORT);
        if (new_region == NULL) return false;
    }
    EXIT_TRACE("setArrayArg");
    return true;
}

JNI_JAVA(jboolean, OpenCLBridge, tryCache)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx,
         jint index, jlong broadcastId, jint rddid, jint partitionid,
         jint offsetid, jint componentid, jint ncomponents) {
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    swat_context *context = (swat_context *)lctx;
    bool all_succeed = true;
    int c = 0;

#ifdef VERBOSE
    fprintf(stderr, "tryCache: index=%d broadcast=%ld rdd=%d partition=%d "
            "offset=%d component=%d ncomponents=%d\n", index, broadcastId,
            rddid, partitionid, offsetid, componentid, ncomponents);
#endif

    ENTER_TRACE("tryCache");
    if (broadcastId >= 0) {
        ASSERT(rddid < 0 && componentid >= 0);

        int err = pthread_rwlock_rdlock(&dev_ctx->broadcast_lock);
        ASSERT(err == 0);
        
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
                TRACE("caching broadcast %d %d\n", broadcastId, componentid + c);
                set_cached_kernel_arg(region, index + c, region->size, context, dev_ctx);
                (*context->arguments)[index + c] = pair<cl_region *, bool>(region, true);
            } else {
                TRACE("failed to try-cache broadcast %d %d\n", broadcastId, componentid + c);
                all_succeed = false;
            }
            c++;
        }
    } else if (rddid >= 0) {
        ASSERT(broadcastId < 0 && partitionid >= 0 && offsetid >= 0 && componentid >= 0);
        int err = pthread_rwlock_rdlock(&rdd_cache_lock);
        ASSERT(err == 0);

        while (all_succeed && c < ncomponents) {
            rdd_partition_offset uuid(rddid, partitionid, offsetid, componentid + c);
            cl_region *region = NULL;
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
            bool reallocated = (region && re_allocate_cl_region(region,
                        dev_ctx->device_index));
            if (reallocated) {
                TRACE("caching rdd=%d partition=%d offset=%d component=%d\n", rddid,
                        partitionid, offsetid, componentid + c);
                set_cached_kernel_arg(region, index + c, region->size, context, dev_ctx);
                (*context->arguments)[index] = pair<cl_region *, bool>(region, true);
            } else {
                TRACE("failed to try-cache rdd=%d partition=%d offset=%d "
                        "component=%d\n", rddid, partitionid, offsetid,
                        componentid + c);
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
        c -= 2;
        while (c >= 0) {
            pair<cl_region *, bool> cached = context->arguments->at(index + c);
            free_cl_region(cached.first, cached.second);
            c--;
        }
    }

    if (broadcastId >= 0) {
        int err = pthread_rwlock_unlock(&dev_ctx->broadcast_lock);
        ASSERT(err == 0);
    } else if (rddid >= 0) {
        int err = pthread_rwlock_unlock(&rdd_cache_lock);
        ASSERT(err == 0);
    }

    EXIT_TRACE("tryCache");
    return all_succeed;
}

JNI_JAVA(void, OpenCLBridge, manuallyRelease)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx,
         jint startingIndexInclusive, jint endingIndexExclusive) {
    swat_context *context = (swat_context *)lctx;
    ENTER_TRACE("manuallyRelease");

    for (int c = startingIndexInclusive; c < endingIndexExclusive; c++) {
        map<int, pair<cl_region *, bool> >::iterator found = context->arguments->find(c);
        if (found != context->arguments->end()) {
            pair<cl_region *, bool> cached = found->second;
            free_cl_region(cached.first, cached.second);
        }
    }

    EXIT_TRACE("manuallyRelease");
}

JNI_JAVA(void, OpenCLBridge, postKernelCleanup)
        (JNIEnv *jenv, jclass clazz, jlong lctx) {
    swat_context *ctx = (swat_context *)lctx;
    postKernelCleanupHelper(ctx);
}

JNI_JAVA(jboolean, OpenCLBridge, setArgUnitialized)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum,
         jlong size) {
    ENTER_TRACE("setArgUnitialized");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    cl_region *region = get_mem_cached(context, dev_ctx, argnum, size, -1, -1);
    if (region == NULL) return false;

    CHECK(clSetKernelArg(context->kernel, argnum, sizeof(region->sub_mem), &region->sub_mem));

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[argnum] = new kernel_arg(NULL, size, true,
            false);
#endif
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

    cl_mem none = 0x0;
    swat_context *context = (swat_context *)lctx;
    CHECK(clSetKernelArg(context->kernel, argnum, sizeof(none), &none));

    (*context->arguments)[argnum] = pair<cl_region *, bool>(NULL, false);

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[argnum] = new kernel_arg(&none, sizeof(none),
            false, false);
#endif
#ifdef VERBOSE
    fprintf(stderr, "setting arg %d to a null value\n", argnum);
#endif
    EXIT_TRACE("setNullArrayArg");
}


JNI_JAVA(int, OpenCLBridge, createHeapImpl)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint argnum, jint size,
         jint max_n_buffered) {
    ENTER_TRACE("createHeap");

    unsigned zero = 0;
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    jint local_argnum = argnum;

    // The heap
    cl_region *region = get_mem_cached(context, dev_ctx, local_argnum, size, -1, -1);
    if (region == NULL) return -1;
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL, size,
            true, false);
#endif

    // free_index
    region = get_mem_cached(context, dev_ctx, local_argnum, sizeof(zero), -1, -1);
    if (region == NULL) return -1;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem, CL_TRUE, 0, sizeof(zero),
                &zero, 0, NULL, NULL));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(zero), true, true);
#endif

    // heap_size
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(size), &size));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(&size,
            sizeof(size), false, false);
#endif

    // processing_succeeded
    region = get_mem_cached(context, dev_ctx, local_argnum, sizeof(int) * max_n_buffered, -1, -1);
    if (region == NULL) return -1;
#ifdef VERBOSE
    fprintf(stderr, "%d: Creating %p, %lu bytes for processing_succeeded\n",
            context->host_thread_index, region->sub_mem,
            sizeof(int) * max_n_buffered);
#endif
    cl_event fill_event;
    int *zeros = (int *)malloc(max_n_buffered * sizeof(int));
    CHECK_ALLOC(zeros)
    memset(zeros, 0x00, max_n_buffered * sizeof(int));
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem, CL_FALSE, 0,
                max_n_buffered * sizeof(int), zeros, 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    CHECK(clWaitForEvents(1, &fill_event));
    free(zeros);
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(int) * max_n_buffered, true, true);
#endif

    // any_failed
    region = get_mem_cached(context, dev_ctx, local_argnum, sizeof(unsigned), -1, -1);
    if (region == NULL) return -1;
#ifdef VERBOSE
    fprintf(stderr, "%d: Creating %p, %lu bytes for any_failed\n",
            context->host_thread_index, region->sub_mem, sizeof(unsigned));
#endif
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, region->sub_mem, CL_FALSE, 0, sizeof(zero),
                &zero, 0, NULL, &fill_event));
    CHECK(clSetKernelArg(context->kernel, local_argnum, sizeof(region->sub_mem), &region->sub_mem));
    CHECK(clWaitForEvents(1, &fill_event));
    local_argnum++;

#ifdef BRIDGE_DEBUG
    (*context->debug_arguments)[local_argnum - 1] = new kernel_arg(NULL,
            sizeof(unsigned), true, true);
#endif
#ifdef VERBOSE
    fprintf(stderr, "Creating heap at %d->%d (inclusive)\n", argnum,
            local_argnum - 1);
#endif
    EXIT_TRACE("createHeap");
    return (local_argnum - argnum); // Number of kernel arguments added
}

JNI_JAVA(void, OpenCLBridge, resetHeap)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint starting_argnum) {
    ENTER_TRACE("resetHeap");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    map<int, pair<cl_region *, bool> > *arguments = context->arguments;
    int free_index_arg_index = starting_argnum + 1;
    int any_failed_arg_index = starting_argnum + 4;

    ASSERT(arguments->find(free_index_arg_index) != arguments->end());
    ASSERT(arguments->find(any_failed_arg_index) != arguments->end());

    cl_region *free_index_mem = arguments->at(free_index_arg_index).first;
    cl_region *any_failed_mem = arguments->at(any_failed_arg_index).first;

    unsigned zero = 0;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, free_index_mem->sub_mem, CL_TRUE, 0,
                sizeof(zero), &zero, 0, NULL, NULL));

    cl_event fill_event;
    CHECK(clEnqueueWriteBuffer(dev_ctx->cmd, any_failed_mem->sub_mem, CL_FALSE, 0,
                sizeof(zero), &zero, 0, NULL, &fill_event));
    CHECK(clWaitForEvents(1, &fill_event));
    EXIT_TRACE("resetHeap");
}

#ifdef BRIDGE_DEBUG
static void save_to_dump_file(swat_context *context) {
    int dump_index = 0;
    int fd;
    do {
        char filename[256];
        sprintf(filename, "bridge.dump.%d", dump_index);
        fd = open(filename, O_WRONLY | O_CREAT, O_EXCL | S_IRUSR | S_IWUSR);
        dump_index++;
    } while(fd == -1);

    // Write kernel source to dump file
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

JNI_JAVA(void, OpenCLBridge, run)
        (JNIEnv *jenv, jclass clazz, jlong lctx, jlong l_dev_ctx, jint range) {
    ENTER_TRACE("run");
    swat_context *context = (swat_context *)lctx;
    device_context *dev_ctx = (device_context *)l_dev_ctx;
    size_t global_range = range;
    cl_event event;

#ifdef BRIDGE_DEBUG
    save_to_dump_file(context);
#endif

#ifdef VERBOSE
    fprintf(stderr, "Host thread %d launching kernel on OpenCL device %s\n",
            context->host_thread_index, get_device_name(dev_ctx->dev));
    print_allocator(dev_ctx->allocator, context->host_thread_index);
#endif

    CHECK(clWaitForEvents(context->n_events, context->events));
    CHECK(clEnqueueNDRangeKernel(dev_ctx->cmd, context->kernel, 1, NULL,
                &global_range, NULL, 0, NULL, &event));
    CHECK(clWaitForEvents(1, &event));
    EXIT_TRACE("run");
}

JNI_JAVA(jlong, OpenCLBridge, nativeMalloc)
        (JNIEnv *jenv, jclass clazz, jlong nbytes) {
    void *ptr = (void *)malloc(nbytes);
    ASSERT(ptr);
    return (jlong)ptr;
}

JNI_JAVA(void, OpenCLBridge, nativeFree)
        (JNIEnv *jenv, jclass clazz, jlong buffer) {
    void *ptr = (void *)buffer;
    free(ptr);
}

JNI_JAVA(jint, OpenCLBridge, getIntFromArray)
        (JNIEnv *jenv, jclass clazz, jlong buffer, jint index) {
    ENTER_TRACE("getIntFromArray");
    int *arr = (int *)buffer;
    EXIT_TRACE("getIntFromArray");
    return arr[index];
}

#define ELE_UNROLLING 16
#define VECTOR_UNROLLING 2

JNI_JAVA(jint, OpenCLBridge, serializeStridedDenseVectorsToNativeBuffer)
        (JNIEnv *jenv, jclass clazz, jlong buffer, jint bufferPosition,
         jlong bufferCapacity, jlong sizesBuffer, jlong offsetsBuffer,
         jint buffered, jint vectorCapacity, jobjectArray vectors,
         jint nToSerialize, jint tiling) {
    ENTER_TRACE("serializeStridedDenseVectorsToNativeBuffer");

    double *serialized = (double *)buffer;
    int *sizes = (int *)sizesBuffer;
    int *offsets = (int *)offsetsBuffer;

    for (int i = 0; i < nToSerialize; i++) {
        const long offset = bufferPosition + i;
        jobject vector = jenv->GetObjectArrayElement(vectors, i);
        CHECK_JNI(vector);

        const jint vectorSize = jenv->CallIntMethod(vector, denseVectorSizeMethod);
        const long lastElement = offset + ((vectorSize - 1) * tiling);
        if (buffered + i >= vectorCapacity || lastElement >= bufferCapacity) {
            EXIT_TRACE("serializeStridedDenseVectorsToNativeBuffer");
            return i;
        }

        // double[] array backing the dense vector
        jarray vectorArray = (jarray)jenv->CallObjectMethod(vector,
                denseVectorValuesMethod);
        CHECK_JNI(vectorArray);

        double *vectorArrayValues = (double *)jenv->GetPrimitiveArrayCritical(
                vectorArray, NULL);
        CHECK_JNI(vectorArrayValues);

        const int niters = vectorSize / ELE_UNROLLING;
        for (int j = 0; j < niters; j += ELE_UNROLLING) {
            double tile[ELE_UNROLLING];
            for (int k = 0; k < ELE_UNROLLING; k++) {
                tile[k] = vectorArrayValues[j + k];
            }

            for (int k = 0; k < ELE_UNROLLING; k++) {
                serialized[offset + ((j + k) * tiling)] = tile[k];
            }
        }
        for (int j = niters * ELE_UNROLLING; j < vectorSize; j++) {
            serialized[offset + (j * tiling)] = vectorArrayValues[j];
        }

        // for (int j = 0; j < vectorSize; j++) {
        //     serialized[offset + (j * tiling)] = vectorArrayValues[j];
        // }

        jenv->ReleasePrimitiveArrayCritical(vectorArray, vectorArrayValues,
                JNI_ABORT);

        sizes[buffered + i] = vectorSize;
        offsets[buffered + i] = offset;
    }
    EXIT_TRACE("serializeStridedDenseVectorsToNativeBuffer");
    return nToSerialize;
}

JNI_JAVA(void, OpenCLBridge, storeNLoaded)(JNIEnv *jenv, jclass clazz, jint rddid,
         jint partitionid, jint offsetid, jint n_loaded) {
    assert(rddid >= 0);

    rdd_partition_offset uuid(rddid, partitionid, offsetid, 0);
    map<rdd_partition_offset, int>::iterator found = nloaded_cache->find(uuid);
    if (found != nloaded_cache->end()) {
        assert(found->second == n_loaded);
    } else {
        bool success = nloaded_cache->insert(pair<rdd_partition_offset, int>(
                    uuid, n_loaded)).second;
        assert(success);
    }
}

JNI_JAVA(jint, OpenCLBridge, fetchNLoaded)(JNIEnv *jenv, jclass clazz, jint rddid,
         jint partitionid, jint offsetid) {
    assert(rddid >= 0);

    rdd_partition_offset uuid(rddid, partitionid, offsetid, 0);
    map<rdd_partition_offset, int>::iterator found = nloaded_cache->find(uuid);
    if (found != nloaded_cache->end()) {
        return found->second;
    } else {
        return -1;
    }
}

#ifdef __cplusplus
}
#endif
