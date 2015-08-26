#ifndef COMMON_H
#define COMMON_H

#include <assert.h>
#include <unistd.h>
#include <stdlib.h>

extern void safe_write(int fd, const void *buf, size_t count);
extern void safe_read(int fd, void *buf, size_t count);

#endif
