#!/usr/bin/python
import os
import sys

class LockInfo:
    def __init__(self):
        self.contention = {}

    def update(self, lbl, t):
        self.contention[lbl] = t


class AllocInfo:
    def __init__(self):
        self.init = 0
        self.alloc = 0
        self.realloc = 0
        self.free = 0

    def update(self, set_init, set_alloc, set_realloc, set_free):
        self.init = set_init
        self.alloc = set_alloc
        self.realloc = set_realloc
        self.free = set_free


if len(sys.argv) != 2:
    print('usage: python gather_lock_info.py filename')
    sys.exit(1)

lock_info = LockInfo()
alloc_info = AllocInfo()

fp = open(sys.argv[1], 'r')
for line in fp:
    if 'clalloc' in line:
        tokens = line.split()
        init = int(tokens[6])
        alloc = int(tokens[11])
        realloc = int(tokens[16])
        free = int(tokens[21])
        alloc_info.update(init, alloc, realloc, free)
    elif line.startswith(' LOCK : '):
        tokens = line.split()
        lbl = tokens[4]
        t = int(tokens[6])
        lock_info.update(lbl, t)

fp.close()

print('Alloc info (ns):')
print('  init = ' + str(alloc_info.init))
print('  alloc = ' + str(alloc_info.alloc))
print('  realloc = ' + str(alloc_info.realloc))
print('  free = ' + str(alloc_info.free))
print('')
print('Lock info (ns):')
for lbl in lock_info.contention.keys():
    print('    ' + lbl + ' : ' + str(lock_info.contention[lbl]))
