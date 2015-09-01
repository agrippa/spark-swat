#ifndef KERNEL_ARG_H
#define KERNEL_ARG_H

#include <string.h>

#ifdef __APPLE__
#include <cl.h>
#else
#include <CL/cl.h>
#endif

#include "common.h"
#include "device_context.h"
#include "ocl_util.h"

#ifdef BRIDGE_DEBUG
class kernel_arg {
    public:
        kernel_arg(int fd) {
            safe_read(fd, &size, sizeof(size));
            safe_read(fd, &has_val, sizeof(has_val));
            if (has_val) {
                val = malloc(size);
                assert(val);
                safe_read(fd, val, size);
            } else {
                val = NULL;
            }
            safe_read(fd, &is_ref, sizeof(is_ref));
            safe_read(fd, &clear_to_zero, sizeof(clear_to_zero));
        }

        kernel_arg(void *set_val, size_t set_size, bool set_is_ref,
                bool set_clear_to_zero) {
            if (set_val == NULL) {
                has_val = false;
                val = NULL;
            } else {
                has_val = true;
                val = malloc(set_size);
                memcpy(val, set_val, set_size);
            }

            size = set_size;
            is_ref = set_is_ref;
            clear_to_zero = set_clear_to_zero;
        }

        kernel_arg(cl_mem mem, size_t set_size, device_context *ctx) {
            has_val = true;
            val = malloc(set_size);
            CHECK(clEnqueueReadBuffer(ctx->cmd, mem, CL_TRUE, 0, set_size, val,
                        0, NULL, NULL));

            size = set_size;
            is_ref = true;
            clear_to_zero = false;
        }

        void *get_val() { return val; }
        size_t get_size() { return size; }
        bool get_is_ref() { return is_ref; }
        bool get_clear_to_zero() { return clear_to_zero; }

        void dump(int fd) {
            safe_write(fd, &size, sizeof(size));
            safe_write(fd, &has_val, sizeof(has_val));
            if (has_val) {
                safe_write(fd, val, size);
            }
            safe_write(fd, &is_ref, sizeof(is_ref));
            safe_write(fd, &clear_to_zero, sizeof(clear_to_zero));
        }

    private:
        void *val;
        bool has_val;
        size_t size;
        bool is_ref;
        bool clear_to_zero;
};
#endif


#endif
