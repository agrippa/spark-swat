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
    def __init__(self, thread):
        self.thread = thread

        self.input_start = -1
        self.input_elapsed = -1

        self.output_start = -1
        self.output_elapsed = -1

        self.kernel_start_time = -1
        self.opencl_events = {}

    def str(self):
        return '{ thread=' + str(self.thread) + ', input_start=' + \
               str(self.input_start) + ', input_elapsed=' + \
               str(self.input_elapsed) + ', output_start=' + \
               str(self.output_start) + ', output_elapsed=' + \
               str(self.output_elapsed) + ', kernel_start_time=' + \
               str(self.kernel_start_time) + ' }'

    def has_input_info(self):
        return self.input_start != -1

    def has_output_start(self):
        return self.output_start != -1

    def has_output_elapsed(self):
        return self.output_elapsed != -1

    def has_kernel_start_info(self):
        return self.kernel_start_time != -1

    def has_opencl_event(self, event_index):
        return event_index in self.opencl_events

    def set_input_time(self, start, elapsed):
        assert self.input_start == -1 and self.input_elapsed == -1
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

    def add_opencl_event(self, thread, index, lbl, total_ns):
        assert self.thread == thread
        event = OpenCLEvent(index, lbl, total_ns)
        assert index not in self.opencl_events.keys()
        self.opencl_events[index] = event

    def get_nevents(self):
        return len(self.opencl_events)

    def complete(self, expected_n_events):
        nevents = len(self.opencl_events)
        assert nevents == expected_n_events
        for i in range(nevents):
            if i not in self.opencl_events.keys():
                return False
        return self.thread >= 0 and \
               self.input_start >= 0 and self.input_elapsed >= 0 and \
               self.output_start >= 0 and self.output_elapsed >= 0 and \
               self.kernel_start_time >= 0


class ThreadKernelInfo:
    def __init__(self, thread):
        self.thread = thread
        self.kernels = [] # information on each kernel instance
        self.lock_info = LockInfo(thread)

    def get_nevents(self):
        expected = self.kernels[0].get_nevents()
        index = 0
        for k in self.kernels:
            assert expected == k.get_nevents(), 'thread ' + str(self.thread) + \
                   ' expected ' + str(expected) + ' events, but got ' + \
                   str(k.get_nevents()) + ' on ' + str(index + 1) + '/' + \
                   str(len(self.kernels))
            index += 1
        return expected

    def check_all_complete(self, expected_n_events):
        for k in self.kernels:
            assert k.complete(expected_n_events), k.str()

    def create_new_kernel(self):
        kernel = KernelInfo(self.thread)
        self.kernels.append(kernel)
        return kernel

    def add_input_info(self, start, elapsed):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_input_info():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_input_time(start, elapsed)

    def add_output_start(self, start):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_output_start():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_output_start(start)

    def add_output_end(self, end):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_output_elapsed():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_output_elapsed(end)

    def add_kernel_start(self, start):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_kernel_start_info():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_kernel_start_time(start)

    def add_opencl_event(self, index, lbl, total_ns):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_opencl_event(index):
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].add_opencl_event(self.thread, index, lbl, total_ns)

    def get_lock_info(self):
        return self.lock_info


per_thread_total_time = {}
per_thread_info = {}

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

if len(sys.argv) != 2:
    print('usage: python collect_detailed_metrics.py filename')
    sys.exit(1)

fp = open(sys.argv[1], 'r')
for line in fp:
    if line.startswith('SWAT PROF') and not line.startswith('SWAT PROF Total loaded'):
        tokens = line.split()
        event = tokens[3]
        thread = int(tokens[2])

        if event == 'Input-I/O':
            input_start = int(tokens[6])
            input_elapsed = int(tokens[4])

            thread_info = get_thread_info(thread)
            kernel_info = thread_info.create_new_kernel()
            thread_info.add_input_info(input_start, input_elapsed)
        elif event == 'Total':
            add_total_time(thread, int(tokens[4]))
        elif event == 'Kernel' and tokens[4] == 'launch':
            seq = int(tokens[10])
            thread_info = get_thread_info(thread)
            start_time = int(tokens[6])
            thread_info.add_kernel_start(start_time)
        elif event == 'Started' and tokens[4] == 'writing':
            t = int(tokens[9])
            thread_info = get_thread_info(thread)
            thread_info.add_output_start(t)
        elif event == 'Finished' and tokens[4] == 'writing':
            t = int(tokens[9])
            thread_info = get_thread_info(thread)
            thread_info.add_output_end(t)
    elif 'queued -> submitted' in line:
        tokens = line.split()
        thread = int(tokens[1])
        seq = int(tokens[4])
        index = int(tokens[6])
        lbl = tokens[8]
        total_ns = int(tokens[10])

        thread_info = get_thread_info(thread)
        thread_info.add_opencl_event(index, lbl, total_ns)
    elif 'LOCK : ' in line:
        tokens = line.split()
        lbl = tokens[4]
        thread = int(tokens[2])

        lock_info = get_lock_info_for_thread(thread)
        t = int(tokens[len(tokens) - 1])
        assert lbl in known_lock_lbls
        lock_info.add_info(lbl, t)

fp.close()

nevents_per_kernel = get_thread_info(0).get_nevents()
print(str(nevents_per_kernel) + ' events per kernel')
get_thread_info(0).check_all_complete(nevents_per_kernel)
