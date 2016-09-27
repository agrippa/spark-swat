#!/bin/bash

set -e

# Output of find_min_image_size.sh:
#
# minWidth=97 minHeight=90
# minWidth=70 minHeight=56
# minWidth=80 minHeight=60
# minWidth=67 minHeight=60
# minWidth=80 minHeight=60
# minWidth=80 minHeight=60
# minWidth=90 minHeight=80
# minWidth=80 minHeight=60
# minWidth=71 minHeight=60
# minWidth=54 minHeight=56
# minWidth=65 minHeight=60
# minWidth=73 minHeight=65

# New output:
# 
# minWidth=97 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2013_test_00008856.JPEG), minHeight=80 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00072427.JPEG)
# minWidth=111 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00000258.JPEG), minHeight=90 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00065033.JPEG)
# minWidth=74 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2013_test_00003257.JPEG), minHeight=80 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00076426.JPEG)
# minWidth=86 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00021777.JPEG), minHeight=101 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00030384.JPEG)
# minWidth=64 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2013_test_00000616.JPEG), minHeight=85 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00047667.JPEG)
# minWidth=100 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00007998.JPEG), minHeight=75 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00007998.JPEG)
# minWidth=44 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00072091.JPEG), minHeight=51 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00072091.JPEG)
# minWidth=100 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00090507.JPEG), minHeight=66 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00090507.JPEG)
# minWidth=90 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00091767.JPEG), minHeight=66 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00044394.JPEG)
# minWidth=83 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00095415.JPEG), minHeight=72 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2013_test_00001620.JPEG)
# minWidth=81 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00007375.JPEG), minHeight=63 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00049392.JPEG)
# minWidth=80 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00005045.JPEG), minHeight=60 (/scratch/jmg3/spark-datasets/imagenet/0.original/ILSVRC2012_test_00005045.JPEG)

MIN_HEIGHT=51
MIN_WIDTH=44

NPROCS=12

mkdir -p $SPARK_DATASETS/imagenet/1.txt

for i in $(seq 0 $(echo $NPROCS - 1 | bc)); do
    /opt/apps/software/Core/Java/1.8.0_45/bin/java \
        -cp $SWAT_HOME/dataset-transformations/imagenet/target/imagenet-0.0.0.jar \
        ConvertJPEGsToText $i $NPROCS $MIN_HEIGHT $MIN_WIDTH $SPARK_DATASETS/imagenet/0.original \
        $SPARK_DATASETS/imagenet/1.txt &
done
wait
