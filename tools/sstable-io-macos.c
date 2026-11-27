/* Optional macOS profiler. Count userspace calls to live SSTable descriptors. */
#include <fcntl.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define FD_LIMIT 4096U

static _Atomic bool table_fds[FD_LIMIT];
static _Atomic unsigned long opens, reads, seeks, bytes_read;

static bool is_table_fd(int fd) {
    return (unsigned)fd < FD_LIMIT &&
           atomic_load_explicit(&table_fds[fd], memory_order_relaxed);
}

static int table_open(const char *path, int flags, ...) {
    int fd;
    if (flags & O_CREAT) {
        va_list arguments;
        va_start(arguments, flags);
        int mode = va_arg(arguments, int);
        va_end(arguments);
        fd = open(path, flags, mode);
    } else {
        fd = open(path, flags);
    }
    size_t len = strlen(path);
    if ((unsigned)fd < FD_LIMIT && len >= 4 &&
        strcmp(path + len - 4, ".sst") == 0) {
        atomic_store_explicit(&table_fds[fd], true, memory_order_relaxed);
        atomic_fetch_add_explicit(&opens, 1, memory_order_relaxed);
    }
    return fd;
}

static ssize_t table_read(int fd, void *buffer, size_t count) {
    ssize_t n = read(fd, buffer, count);
    if (is_table_fd(fd)) {
        atomic_fetch_add_explicit(&reads, 1, memory_order_relaxed);
        if (n > 0) {
            atomic_fetch_add_explicit(&bytes_read, (unsigned long)n, memory_order_relaxed);
        }
    }
    return n;
}

static off_t table_seek(int fd, off_t offset, int whence) {
    if (is_table_fd(fd)) {
        atomic_fetch_add_explicit(&seeks, 1, memory_order_relaxed);
    }
    return lseek(fd, offset, whence);
}

static int table_close(int fd) {
    if (is_table_fd(fd)) {
        atomic_store_explicit(&table_fds[fd], false, memory_order_relaxed);
    }
    return close(fd);
}

__attribute__((destructor))
static void print_counts(void) {
    fprintf(stderr, "sstable_io opens=%lu reads=%lu seeks=%lu bytes_read=%lu\n",
            atomic_load_explicit(&opens, memory_order_relaxed),
            atomic_load_explicit(&reads, memory_order_relaxed),
            atomic_load_explicit(&seeks, memory_order_relaxed),
            atomic_load_explicit(&bytes_read, memory_order_relaxed));
}

#define INTERPOSE(replacement, original)                                       \
    __attribute__((used)) static const struct {                               \
        const void *new_function;                                             \
        const void *old_function;                                             \
    } replace_##original __attribute__((section("__DATA,__interpose"))) = {   \
        (const void *)&replacement, (const void *)&original                   \
    }

INTERPOSE(table_open, open);
INTERPOSE(table_read, read);
INTERPOSE(table_seek, lseek);
INTERPOSE(table_close, close);
