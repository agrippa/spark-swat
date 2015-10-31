#!/usr/bin/python

import os
import sys

# TODO lock info parsing, does it reset on every new partition per thread?

known_lock_lbls = ['device_ctxs_lock_contention',
                   'rdd_cache_lock_contention',
                   'kernel_lock_contention',
                   'freed_native_input_buffers_lock_contention',
                   'freed_native_input_buffers_blocked',
                   'completed_kernels_lock_contention',
                   'completed_kernels_blocked',
                   'broadcast_lock_contention',
                   'program_cache_lock_contention',
                   'heap_cache_lock_contention',
                   'heap_cache_blocked',
                   'nloaded_cache_lock_contention',
                   'allocator_contention']


class OpenCLEvent:
    def __init__(self, index, lbl, total_ns):
        self.index = index
        self.lbl = lbl
        self.total_ns = total_ns


class LockInfo:
    def __init__(self, thread):
        self.thread = thread
        self.info = {}

    def add_info(self, lbl, t):
        self.info[lbl] = t


class KernelInfo:
    def __init__(self, thread, seq):
        self.thread = thread
        self.seq = seq

        self.input_start = -1
        self.input_elapsed = -1

        self.output_start = -1
        self.output_elapsed = -1

        self.kernel_start_time = -1
        self.opencl_events = {}

    def set_input_time(self, start, elapsed):
        assert self.input_start == -1
        self.input_start = start
        self.input_elapsed = elapsed

    def set_output_start(self, start):
        assert self.output_start == -1
        self.output_start = start

    def set_output_elapsed(self, end):
        assert self.output_start != -1 and self.output_elapsed == -1
        self.output_elapsed = end - self.output_start

    def set_kernel_start_time(self, s):
        assert self.kernel_start_time == -1
        self.kernel_start_time = s

    def add_opencl_event(self, thread, seq, index, lbl, total_ns):
        assert self.thread == thread
        assert self.seq == seq, 'expected ' + str(self.seq) + ' but got ' + str(seq)
        event = OpenCLEvent(index, lbl, total_ns)
        assert index not in self.opencl_events.keys()
        self.opencl_events[index] = event

    def complete(self):
        nevents = len(self.opencl_events)
        for i in range(nevents):
            if i not in self.opencl_events.keys():
                return False
        return self.thread >= 0 and self.seq >= 0 and \
               self.input_start >= 0 and self.input_elapsed >= 0 and \
               self.output_start >= 0 and self.output_elapsed >= 0 and \
               self.kernel_start_time >= 0


class ThreadKernelInfo:
    def __init__(self, thread):
        self.thread = thread
        self.kernels = {} # map from seq to kernel info
        self.lock_info = LockInfo(thread)

    def get_kernel_info(self, seq):
        if not seq is in self.kernels.keys():
            self.kernels[seq] = KernelInfo(self.thread, seq)
        return self.kernels[seq]

    def get_lock_info(self):
        return self.lock_info


per_thread_total_time = {}
per_thread_info = {}
per_thread_seq = {}

def get_thread_info(thread):
    if thread not in per_thread_info.keys():
        per_thread_info[thread] = ThreadKernelInfo(thread)
    return per_thread_info[thread]

def get_lock_info_for_thread(thread):
    return get_thread_info(thread).get_lock_info()

def add_total_time(thread, t):
    if thread not in per_thread_total_time:
        per_thread_total_time[thread] = 0
    per_thread_total_time[thread] = per_thread_total_time[thread] + t

def add_completed_kernel(kernel_info):
    if kernel_info.thread not in per_thread_completed_kernels:
        per_thread_completed_kernels[kernel_info.thread] = []
    per_thread_completed_kernels[kernel_info.thread].append(kernel_info)

def incr_and_get_seq(thread):
    if thread not in per_thread_seq.keys():
        per_thread_seq[thread] = 0
        return 0
    else:
        curr_seq = per_thread_seq[thread] + 1
        per_thread_seq[thread] = curr_seq
        return curr_seq


if len(sys.argv) != 2:
    print('usage: python collect_detailed_metrics.py filename')
    sys.exit(1)

fp = open(sys.argv[1], 'r')
for line in fp:
    if line.startswith('SWAT PROF'):
        tokens = line.split()
        event = tokens[3]
        thread = int(tokens[2])

        if event == 'Input-I/O':
            curr_seq = incr_and_get_seq(thread)
            input_start = int(tokens[6])
            input_elapsed = int(tokens[4])

            thread_info = get_thread_info(thread)
            kernel_info = thread_info.get_kernel_info(curr_seq)

            kernel_info.set_input_time(input_start, input_elapsed)
        elif event == 'Total':
            add_total_time(thread, int(tokens[4]))
        elif event == 'Kernel' and tokens[4] == 'launch':
            seq = int(tokens[10])
            thread_info = get_thread_info(thread)
            kernel_info = thread_info.get_kernel_info(seq)
            start_time = int(tokens[6])
            kernel_info.set_kernel_start_time(start_time)
    elif 'queued -> submitted' in line:
        tokens = line.split()
        thread = int(tokens[1])
        seq = int(tokens[4])
        index = int(tokens[6])
        lbl = tokens[8]
        total_ns = int(tokens[10])

        thread_info = get_thread_info(thread)
        kernel_info = thread_info.get_kernel_info(seq)
        kernel_info.add_opencl_event(thread, seq, index, lbl, total_ns)
    elif 'LOCK : ' in line:
        tokens = line.split()
        lbl = tokens[4]
        thread = int(tokens[2])

        lock_info = get_lock_info_for_thread(thread)
        t = int(tokens[len(tokens) - 1])
        assert lbl in known_lock_lbls
        lock_info.add_info(lbl, t)

fp.close()
