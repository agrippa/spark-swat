#!/usr/bin/python

import os
import sys

for line in sys.stdin:
    print('    "' + line[:len(line) - 1] + '\\n" +')
