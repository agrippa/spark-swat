#!/usr/bin/python
import os
import sys
import shutil

if len(sys.argv) < 3:
    print('usage: python profile_toggle.py <enable|disable> input1 input2 ...')
    sys.exit(1)

enable = False
if sys.argv[1] == 'enable':
    enable = True
elif sys.argv[1] == 'disable':
    enable = False
else:
    print('Unexpected argument "' + sys.argv[1] + '", expected enable or disable')
    sys.exit(1)

input_filenames = sys.argv[2:]

for input_filename in input_filenames:
    output_filename = input_filename + '.tmp'

    input_fp = open(input_filename, 'r')
    output_fp = open(output_filename, 'w')

    for line in input_fp:
        if line.strip().endswith('// PROFILE'):
            if enable and line.startswith('// '): # disabled
                output_fp.write(line[3:])
            elif not enable and not line.startswith('// '): # enabled
                output_fp.write('// ' + line)
            else:
                output_fp.write(line)
        else:
            output_fp.write(line)

    input_fp.close()
    output_fp.close()

    shutil.move(output_filename, input_filename)
