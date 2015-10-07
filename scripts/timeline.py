import numpy as np
import matplotlib.pyplot as plt
import sys
import os

class Task:
    def __init__(self, start, elapsed, lbl):
        self.start = start
        self.elapsed = elapsed
        self.lbl = lbl

    def normalize_start(self, min_time):
        self.start = self.start - min_time


colors = [ 'r', 'y', 'b', 'g', 'c', 'm' ]

max_timestamp = 0
min_timestamp = -1
nthreads = 0
if len(sys.argv) != 2:
    print 'usage: python plot.py filename'
    sys.exit()

fp = open(sys.argv[1], 'r')

nthreads_per_worker = 2
supported_labels = {'Input-I/O'             : 0,
                    'Run'                   : 1,
                    'Write'                 : 1,
                    'Read'                  : 1,
                    'KernelAttemptCallback' : 1}
tasks = {}

for line in fp:
    line = line[:len(line)-1]
    tokens = line.split()

    if tokens[0] != 'SWAT':
        continue

    if tokens[2] != 'PROF':
        continue

    tid = int(tokens[3])
    lbl = tokens[4]

    if lbl not in supported_labels.keys():
        continue

    if tid not in tasks.keys():
        tasks[tid] = {}
        i = 0
        while (i < threads_per_worker):
            tasks[tid][i] = []
            i += 1

    elapsed = int(tokens[5])
    finish = int(tokens[7])

    tasks[tid][supported_labels[lbl]].append(Task(finish - elapsed, elapsed, lbl))

    nthreads = max(nthreads, tid + 1)
    max_timestamp = max(max_timestamp, finish)
    if min_timestamp == -1:
        min_timestamp = finish - elapsed
    else:
        min_timestamp = min(min_timestamp, finish - elapsed)

fig = plt.figure(num=0, figsize=(18, 6), dpi=80)

width = 0.35       # the width of the bars: can also be len(x) sequence
color_counter = 0
ind = 0

thread_labels = []
sorted_tids = sorted(tasks.keys())

for tid in sorted_tids:
    for i in sorted_tids[tid]:
        thread_labels.append(str(tid) + ':' + str(i))
        for t in sorted_tids[tid][i]:
            t.normalize_start(min_timestamp)

            plt.barh(ind, t.elapsed, height=width, left=t.start, linewidth=1,
                     color=colors[color_counter])

        ind = ind + width

plt.ylabel('Tasks')
plt.xlabel('Time')
plt.yticks(np.arange(0, nthreads * nthreads_per_worker, width) + width/2.,
           thread_labels)
# plt.xticks(np.arange(min_timestamp, max_timestamp, 10000))
# plt.axis([ min_timestamp, max_timestamp, 0, 5 ])
plt.axis([ 0, max_timestamp-min_timestamp, 0, ind ])
# plt.legend( (p1[0], p2[0]), ('Men', 'Women') )
plt.show()

