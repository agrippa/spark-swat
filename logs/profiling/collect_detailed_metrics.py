#!/usr/bin/python

import os
import sys

# TODO lock info parsing, does it reset on every new partition per thread?

class OpenCLEvent:
    def __init__(self, index, lbl, total_ns):
        self.index = index
        self.lbl = lbl
        self.total_ns = total_ns


class KernelInfo:
    def __init__(self, thread, seq, input_time):
        self.thread = thread
        self.seq = seq
        self.input_time = input_time
        self.kernel_start_time = -1
        self.opencl_events = {}

    def set_kernel_start_time(self, s):
        self.kernel_start_time = s

    def add_opencl_event(self, thread, seq, index, lbl, total_ns):
        assert self.thread == thread
        assert self.seq == seq
        event = OpenCLEvent(index, lbl, total_ns)
        assert index not in self.opencl_events.keys()
        self.opencl_events[index] = event

    def complete(self):
        return self.thread >= 0 and self.seq >= 0 and \
               self.kernel_start_time != -1


per_thread_lock_info = {}
per_thread_total_time = {}
per_thread_completed_kernels = {}
per_thread_kernel = {}
per_thread_seq = {}

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
            input_time = int(tokens[5])

            kernel_info = KernelInfo(thread, curr_seq, input_time)
            if thread in per_thread_kernel:
                assert per_thread_kernel[thread].complete()
                add_completed_kernel(per_thread_kernel[thread])

            per_thread_kernel[thread] = kernel_info
        elif event == 'Total':
            add_total_time(thread, int(tokens[4]))
        elif event == 'Kernel' and tokens[4] == 'launch':
            per_thread_kernel[thread].set_kernel_start_time(int(tokens[6]))
    elif 'queued -> submitted' in line:
        tokens = line.split()
        thread = int(tokens[1])
        seq = int(tokens[4])
        index = int(tokens[6])
        lbl = tokens[8]
        total_ns = int(tokens[10])
        per_thread_kernel[thread].add_opencl_event(thread, seq, index, lbl,
                                                   total_ns)

fp.close()
