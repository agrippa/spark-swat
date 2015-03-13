#ifndef KERNEL_ARG_H
#define KERNEL_ARG_H

#include <string.h>

#ifdef BRIDGE_DEBUG
class kernel_arg {
    public:
        kernel_arg(int fd) {
            safe_read(fd, &size, sizeof(size));
            safe_read(fd, &has_val, sizeof(has_val));
            if (has_val) {
                val = malloc(size);
                safe_read(fd, val, size);
            }
            safe_read(fd, &is_ref, sizeof(is_ref));
            safe_read(fd, &is_memset, sizeof(is_memset));
            safe_read(fd, &memset_value, sizeof(memset_value));
        }

        kernel_arg(void *set_val, size_t set_size, bool set_is_ref,
                bool set_is_memset, int set_memset_value) {
            if (set_val == NULL) {
                val = NULL;
                has_val = false;
            } else {
                has_val = true;
                val = malloc(set_size);
                memcpy(val, set_val, set_size);
            }

            size = set_size;
            is_ref = set_is_ref;
            is_memset = set_is_memset;
            memset_value = set_memset_value;
        }

        void *get_val() { return val; }
        size_t get_size() { return size; }
        bool get_is_ref() { return is_ref; }
        bool get_is_memset() { return is_memset; }
        int get_memset_value() { return memset_value; }

        void set_memset_value(int val) { memset_value = val; is_memset = true; }

        void dump(int fd) {
            safe_write(fd, &size, sizeof(size));
            safe_write(fd, &has_val, sizeof(has_val));
            if (has_val) {
                safe_write(fd, val, size);
            }
            safe_write(fd, &is_ref, sizeof(is_ref));
            safe_write(fd, &is_memset, sizeof(is_memset));
            safe_write(fd, &memset_value, sizeof(memset_value));
        }

    private:
        void *val;
        bool has_val;
        size_t size;
        bool is_ref;
        bool is_memset;
        int memset_value;
};
#endif


#endif
