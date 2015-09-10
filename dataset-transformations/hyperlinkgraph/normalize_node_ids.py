#!/usr/bin/python

import sys
import os

if len(sys.argv) != 2:
    print('usage: normalize_node_ids.py <input-dir> <output-dir>')
    sys.exit(1)

input_dir = sys.argv[0]
output_dir = sys.argv[1]

unique = Set()
input_files = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
for f in input_files:
    print('Processing ' + f)
    tokens = f.split()
    assert len(tokens) == 2
    unique.add(int(tokens[0]))
    unique.add(int(tokens[1]))

nunique = len(unique)
print str(nunique) + ' unique elements'

