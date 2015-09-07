#!/usr/bin/python

import gzip
import cPickle
import sys
import shutil
import os
from os import listdir
from os.path import isfile, join
import random

def clean_dir(dirname):
    for file in os.listdir(dirname):
        path = os.path.join(dirname, file)
        if os.path.isfile(path):
            os.unlink(path)


def convert_to_txt(nimages, images, classifications, image_dir, classification_dir,
        nclasses, per_file, target_n_images):
    zeros = [0] * nclasses
    file = 0
    sofar = 0
    img_fp = open(os.path.join(image_dir, 'input.' + str(file)), 'w')
    classification_fp = open(os.path.join(classification_dir, 'input.' + str(file)), 'w')

    i = 0
    while i < target_n_images:
        img = images[i % nimages]
        cls = classifications[i % nimages]

        img_fp.write(str(i))
        for pixel in img:
            img_fp.write(' ' + str(pixel))
        img_fp.write('\n')

        classification_fp.write(str(i))
        zeros[cls] = 1
        for val in zeros:
            classification_fp.write(' ' + str(val))
        classification_fp.write('\n')
        zeros[cls] = 0

        sofar += 1

        if sofar == per_file:
            sofar = 0
            file += 1
            img_fp.close()
            classification_fp.close()

            if i != target_n_images - 1:
                img_fp = open(os.path.join(image_dir, 'input.' + str(file)), 'w')
                classification_fp = open(os.path.join(classification_dir, 'input.' + str(file)), 'w')
            else:
                img_fp = None
                correct_cp = None

        i += 1

    if img_fp is not None:
        img_fp.close
        classification_fp.close

    return file


if len(sys.argv) < 8:
    print('usage: generate.py path-to-mnist.pkl.gz limit-data per-file training-dir ' +
          'training-correct-dir testing-dir testing-correct-dir info-file ' +
          'nclasses target-n-training-image-files layer1dim layer2dim ...')
    sys.exit(1)

f = gzip.open(sys.argv[1], 'rb')
training_data, validation_data, test_data = cPickle.load(f)
f.close()

limit = int(sys.argv[2])
per_file = int(sys.argv[3])
training_dir = sys.argv[4]
correct_dir = sys.argv[5]
testing_dir = sys.argv[6]
testing_correct_dir = sys.argv[7]
info_file = sys.argv[8]
nclasses = int(sys.argv[9])
target_n_training_image_files = int(sys.argv[10])
layer_str = sys.argv[11:]

clean_dir(training_dir)
clean_dir(correct_dir)
clean_dir(testing_dir)
clean_dir(testing_correct_dir)

testing_images = test_data[0]
testing_classification = test_data[1]

training_images = training_data[0]
classification = training_data[1]
assert len(training_images) == len(classification)
zeros = [0] * nclasses

layers = []
layers.append(len(training_images[0]))
for layer in layer_str:
    layers.append(int(layer))
layers.append(nclasses)

print(str(len(training_images)) + ' training images')
print(str(len(testing_images)) + ' testing images')

print('Converting training data...')
convert_to_txt(len(training_images) if limit == -1 else limit, training_images,
               classification, training_dir, correct_dir, nclasses, per_file,
               target_n_training_image_files * per_file)

print('Converting testing data...')
n_testing_images = convert_to_txt(len(testing_images), testing_images,
                                  testing_classification, testing_dir,
                                  testing_correct_dir, nclasses, per_file,
                                  len(testing_images))
print('Done!')

info_fp = open(info_file, 'w')
for l in layers:
    info_fp.write(str(l) + '\n')
info_fp.close()
