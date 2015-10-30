#!/usr/bin/python

import os
import sys


class KernelInfo:
    def __init__(self, thread, seq, input_time):
        self.thread = thread
        self.seq = seq
        self.input_time = input_time

    def complete(self):
        return self.thread >= 0 and self.seq >= 0


completed_kernels = []
per_thread_kernel = {}
per_thread_seq = {}

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
                completed_kernels.append(per_thread_kernel[thread])

            per_thread_kernel[thread] = kernel_info



fp.close()
