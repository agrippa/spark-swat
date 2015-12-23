#!/usr/bin/python
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os
import sys
from common import parse_timeline

if len(sys.argv) != 3:
    print('usage: python plot_input_output_opencl.py filename limit-n-threads')
    sys.exit(1)

per_thread_info = parse_timeline(sys.argv[1])
limit_n_threads = int(sys.argv[2])

fig = plt.figure(num=0, figsize=(18, 6), dpi=80)
# colors = [ 'r', 'y', 'b', 'g', 'c', 'm' ]
colors = [ '#000000', '#cccccc', '#777777' ]

width = 0.35       # the width of the bars: can also be len(x) sequence
color_counter = 0
ind = 0

min_timestamp = -1
max_timestamp = 0
for thread in per_thread_info:
    thread_info = per_thread_info[thread]
    for kernel in thread_info.get_kernel_list():
      if min_timestamp == -1 or min_timestamp > kernel.get_input_start():
          min_timestamp = kernel.get_input_start()
      write_end = kernel.get_output_start() + kernel.get_output_elapsed()
      if write_end > max_timestamp:
          max_timestamp = write_end

thread_labels = []

thread_iters = per_thread_info.keys()
if limit_n_threads != -1:
    thread_iters = range(limit_n_threads)

for target_thread in thread_iters:
    thread_info = per_thread_info[target_thread]
    print('Thread ' + str(target_thread) + ', total time = ' +
          str(thread_info.total_time) + ' ms')
    print('  ' + str(len(thread_info.get_kernel_list())) + ' kernels')
    thread_labels.append('T' + str(target_thread) + '-input')
    thread_labels.append('T' + str(target_thread) + '-output')
    thread_labels.append('T' + str(target_thread) + '-opencl')

    for kernel in thread_info.get_kernel_list():
        normalized_read_start = kernel.get_input_start() - min_timestamp
        read_elapsed = kernel.get_input_elapsed()
    
        plt.barh(ind, read_elapsed, height=width, left=normalized_read_start,
                 linewidth=1, color=colors[0])

    ind = ind + width

    for kernel in thread_info.get_kernel_list():
        opencl_start = -1
        opencl_end = -1
        for event in kernel.get_event_list():
            if opencl_start == -1 or event.started_ms < opencl_start:
                opencl_start = event.started_ms
            if opencl_end == -1 or event.started_ms + event.total_ms > opencl_end:
                opencl_end = event.started_ms + event.total_ms
        plt.barh(ind, opencl_end - opencl_start, height=width,
                 left=opencl_start - min_timestamp, linewidth=1, color=colors[2])

    ind = ind + width

    for kernel in thread_info.get_kernel_list():
        normalized_write_start = kernel.get_output_start() - min_timestamp
        write_elapsed = kernel.get_output_elapsed()

        plt.barh(ind, write_elapsed, height=width, left=normalized_write_start,
                 linewidth=1, color=colors[1])

    ind = ind + width

print('Used ' + colors[0] + ' for reads, ' + colors[2] + ' for opencl, ' +
      colors[1] + ' for writes')

plt.ylabel('Work')
plt.xlabel('Time (ms)')
plt.yticks(np.arange(0, len(per_thread_info) * 3, width) + width/2., thread_labels)

plt.axis([ 0, max_timestamp-min_timestamp, 0, ind ])

plt.show()
