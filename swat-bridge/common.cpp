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

#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

void safe_write(int fd, const void *buf, size_t count) {
    ssize_t written = write(fd, buf, count);
    if (written == -1) {
        fprintf(stderr, "Write failed\n");
        perror("write");
        exit(1);
    } else if ((size_t)written != count) {
        fprintf(stderr, "Expected a write of %lu bytes but actually wrote %lu "
                "bytes\n", count, written);
        exit(1);
    }
}

void safe_read(int fd, void *buf, size_t count) {
    ssize_t r = read(fd, buf, count);
    if (r == -1) {
        fprintf(stderr, "Read failed\n");
        perror("read");
        exit(1);
    } else if ((size_t)r != count) {
        fprintf(stderr, "Expected a read of %lu bytes but actually read %lu "
                "bytes\n", count, r);
        exit(1);
    }
}

