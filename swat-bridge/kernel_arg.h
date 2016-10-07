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

#ifndef KERNEL_ARG_H
#define KERNEL_ARG_H

#include <string.h>

#ifdef USE_CUDA
#else
#include <CL/cl.h>
#endif

#include "common.h"
#include "device_context.h"
#include "ocl_util.h"

#ifdef BRIDGE_DEBUG
class kernel_arg {
    public:
        // Constructor for loading an argument from a stored checkpoint.
        kernel_arg(int fd) {
            safe_read(fd, &size, sizeof(size));
            safe_read(fd, &has_val, sizeof(has_val));
            if (has_val) {
                val = malloc(size);
                assert(val);
                safe_read(fd, &mem, sizeof(mem));
                safe_read(fd, val, size);
            } else {
                val = NULL;
            }
            safe_read(fd, &is_ref, sizeof(is_ref));
            safe_read(fd, &clear_to_zero, sizeof(clear_to_zero));
        }

        // Constructor for scalar parameters
        kernel_arg(void *set_val, size_t set_size) {
            assert(set_val);

            has_val = true;
            val = malloc(set_size);
            memcpy(val, set_val, set_size);

            size = set_size;
            is_ref = false;
            clear_to_zero = false;
        }

        // Constructor for reference/array parameters
        kernel_arg(cl_mem set_mem, size_t set_size, device_context *ctx) {
            has_val = true;
            mem = set_mem;
            val = malloc(set_size);
            ASSERT(val);
#ifdef VERBOSE
            fprintf(stderr, "Transferring out %lu bytes for kernel arg\n", set_size);
#endif

#ifdef USE_CUDA
            CHECK_DRIVER(cuCtxPushCurrent(ctx->ctx));
            CHECK_DRIVER(cuMemcpyDtoH(val, set_mem, set_size));
            CUcontext old;
            CHECK_DRIVER(cuCtxPopCurrent(&old));
            assert(ctx->ctx == old);
#else
            CHECK(clEnqueueReadBuffer(ctx->cmd, set_mem, CL_TRUE, 0, set_size, val,
                        0, NULL, NULL));
#endif

            size = set_size;
            is_ref = true;
            clear_to_zero = false;
        }

        void *get_val() { return val; }
        size_t get_size() { return size; }
        bool get_is_ref() { return is_ref; }
        bool get_clear_to_zero() { return clear_to_zero; }
        cl_mem get_mem() { return mem; }

        void dump(int fd) {
            safe_write(fd, &size, sizeof(size));
            safe_write(fd, &has_val, sizeof(has_val));
            if (has_val) {
                safe_write(fd, &mem, sizeof(mem));
                safe_write(fd, val, size);
            }
            safe_write(fd, &is_ref, sizeof(is_ref));
            safe_write(fd, &clear_to_zero, sizeof(clear_to_zero));
        }

    private:
        cl_mem mem;
        void *val;
        bool has_val;
        size_t size;
        bool is_ref;
        bool clear_to_zero;
};
#endif


#endif
