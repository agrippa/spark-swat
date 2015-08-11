#ifndef COMMON_H
#define COMMON_H

#include <assert.h>
#include <unistd.h>
#include <stdlib.h>

inline void safe_write(int fd, const void *buf, size_t count) {
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
inline void safe_read(int fd, void *buf, size_t count) {
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

#endif
