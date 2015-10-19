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


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


colors = [ 'r', 'y', 'b', 'g', 'c', 'm' ]

max_timestamp = 0
min_timestamp = -1
nthreads = 0
if len(sys.argv) != 2:
    print 'usage: python plot.py filename'
    sys.exit()

fp = open(sys.argv[1], 'r')

nthreads_per_worker = 2
supported_labels = {'Input-I/O'             : [0, 'r', 'Red'],
                    'Run'                   : [1, 'y', 'Yellow'],
                    'Write'                 : [1, 'b', 'Blue'],
                    'Read'                  : [1, 'g', 'Green'],
                    'KernelAttemptCallback' : [1, 'c', 'Cyan']}
tasks = {}

for line in fp:
    line = line[:len(line)-1]
    tokens = line.split()

    if len(tokens) < 7:
        continue

    if tokens[0] != 'SWAT':
        continue

    if tokens[1] != 'PROF':
        continue

    if not is_int(tokens[2]):
        continue

    tid = int(tokens[2])
    lbl = tokens[3]

    if lbl not in supported_labels.keys():
        continue

    if tid not in tasks.keys():
        tasks[tid] = {}
        i = 0
        while (i < nthreads_per_worker):
            tasks[tid][i] = []
            i += 1

    elapsed = int(tokens[4])
    finish = int(tokens[6])

    tasks[tid][supported_labels[lbl][0]].append(Task(finish - elapsed, elapsed, lbl))

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

print('min_timestamp=' + str(min_timestamp) + ', max_timestamp=' +
      str(max_timestamp))
for lbl in supported_labels.keys():
    print(lbl + ' -> ' + supported_labels[lbl][2])

for tid in sorted_tids:
    for i in tasks[tid]:
        thread_labels.append(str(tid) + ':' + str(i))
        for t in tasks[tid][i]:
            t.normalize_start(min_timestamp)

            plt.barh(ind, t.elapsed, height=width, left=t.start, linewidth=1,
                     color=supported_labels[t.lbl][1])

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
