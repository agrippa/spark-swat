#!/usr/bin/python
import os
import sys

# SWAT PROF 0 Kernel launch @ 1446313441338 for seq = 15  thread 8 : seq 11 : 7 : out : 832448 ns total (started = 29149092533, queued -> submitted 4864 ns, submitted -> started 701568 ns, started -> finished 126016 ns)
for line in sys.stdin:
    if line.startswith('SWAT PROF') and line.find('  thread ') != -1:
        index = line.find('  thread ')
        sys.stdout.write(line[0:index] + '\n')
        sys.stdout.write(line[index:])
    else:
        sys.stdout.write(line)
