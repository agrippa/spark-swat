#!/usr/bin/python

import os
import sys

class OpenCLEventInfo:
    def __init__(self, tokens):
        assert tokens[5] == 'total'
        self.total = int(tokens[3])
        timestamp_str = tokens[8]
        self.timestamp = int(timestamp_str[0:len(timestamp_str) - 1])
        self.queued = int(tokens[12])
        self.submitted = int(tokens[17])
        self.run = int(tokens[22])


PADDING = 30
def pad(s):
    assert len(s) <= PADDING
    padding = PADDING - len(s)
    return s + (' ' * padding)


known_labels = ['0: init_write :',
                '1: init_write :',
                '2: init_write :',
                '3: init_write :',
                '4: free_index-in :',
                '5: run :',
                '6: free_index-out :',
                '7: heap :',
                '8: out :',
                '9: out :']

labels_info = {}
for lbl in known_labels:
    labels_info[lbl] = []

if len(sys.argv) != 2:
    print('usage: grep_opencl_stats.py filename')
    sys.exit(1)

fp = open(sys.argv[1], 'r')
for line in fp:
    tokens = line.split()
    if len(tokens) < 3:
        continue

    lbl = ' '.join(tokens[0:3])
    if lbl in labels_info.keys():
        info = OpenCLEventInfo(tokens)
        labels_info[lbl].append(info)

print(pad('LABEL') + pad('AVG TOTAL') + pad('AVG QUEUED') +
      pad('AVG SUBMITTED') + pad('AVG RUN'))
nlabels = float(len(known_labels))

sums = [0.0, 0.0, 0.0, 0.0]
for lbl in known_labels:
    infos = labels_info[lbl]
    ninfos = float(len(infos))

    sum_totals = 0.0
    sum_queued = 0.0
    sum_submitted = 0.0
    sum_run = 0.0
    for i in infos:
        sum_totals += i.total
        sum_queued += i.queued
        sum_submitted += i.submitted
        sum_run += i.run

    sums[0] += sum_totals
    sums[1] += sum_queued
    sums[2] += sum_submitted
    sums[3] += sum_run

    print(pad(lbl) + pad(str(sum_totals / ninfos)) +
          pad(str(sum_queued / ninfos)) + pad(str(sum_submitted / ninfos)) +
          pad(str(sum_run / ninfos)))
print(pad('') + pad('==========') + pad('==========') + pad('==========') + pad('=========='))
print(pad('') + pad(str(sums[0] / nlabels)) + pad(str(sums[1] / nlabels)) +
      pad(str(sums[2] / nlabels)) + pad(str(sums[3] / nlabels)))

fp.close()
