#!/usr/bin/python

import gzip
import cPickle
import sys
import os


def clean_dir(dirname):
    for file in os.listdir(dirname):
        path = os.path.join(dirname, file)
        if os.path.isfile(path):
            os.unlink(path)


if len(sys.argv) < 8:
    print('usage: generate.py path-to-mnist.pkl.gz per-file training-dir ' +
          'correct-dir info-file nclasses nlayers layer1dim layer2dim ...')
    sys.exit(1)

f = gzip.open(sys.argv[1], 'rb')
training_data, validation_data, test_data = cPickle.load(f)
f.close()

per_file = int(sys.argv[2])
training_dir = sys.argv[3]
correct_dir = sys.argv[4]
info_file = sys.argv[5]
nclasses = int(sys.argv[6])
nlayers = int(sys.argv[7])
layer_str = sys.argv[8:]

clean_dir(training_dir)
clean_dir(correct_dir)

images = training_data[0]
classification = training_data[1]
assert len(images) == len(classification)
zeros = [0] * nclasses

layers = []
layers.append(len(images[0]))
for layer in layer_str:
    layers.append(int(layer))
layers.append(nclasses)

print(str(len(images)) + ' images')

file = 0
sofar = 0
training_fp = open(os.path.join(training_dir, 'input.' + str(file)), 'w')
correct_fp = open(os.path.join(correct_dir, 'input.' + str(file)), 'w')
print('writing file ' + str(file))
for i in xrange(0, len(images)):
    img = images[i]
    cls = classification[i]

    for pixel in img:
        training_fp.write(str(pixel) + ' ')
    training_fp.write('\n')

    zeros[cls] = 1
    for val in zeros:
        correct_fp.write(str(val) + ' ')
    correct_fp.write('\n')
    zeros[cls] = 0

    sofar += 1
    if sofar == per_file:
        sofar = 0
        file += 1
        training_fp.close()
        correct_fp.close()

        if i != len(images) - 1:
            training_fp = open(os.path.join(training_dir, 'input.' + str(file)), 'w')
            correct_fp = open(os.path.join(correct_dir, 'input.' + str(file)), 'w')
            print('writing file ' + str(file))
        else:
            training_fp = None
            correct_cp = None

if training_fp is not None:
    training_fp.close
    correct_fp.close

info_fp = open(info_file, 'w')
for l in layers:
    info_fp.write(str(l) + '\n')
info_fp.close()
