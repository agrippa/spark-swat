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

MIN_HEIGHT=56
MIN_WIDTH=54

NPROCS=12

for i in $(seq 0 $(echo $NPROCS - 1 | bc)); do
    /opt/apps/software/Core/Java/1.8.0_45/bin/java \
        -cp $SWAT_HOME/dataset-transformations/imagenet/target/imagenet-0.0.0.jar \
        ConvertJPEGsToText $i $NPROCS $MIN_HEIGHT $MIN_WIDTH $SPARK_DATASETS/imagenet/0.original \
        $SPARK_DATASETS/imagenet/1.txt &
done
wait
