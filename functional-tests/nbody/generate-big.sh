#!/bin/bash

scala -classpath ./target/sparknbody-0.0.0.jar GenerateInput /scratch/jmg3/spark-inputs/nbody/points 100 \
          1000 /scratch/jmg3/spark-inputs/nbody/info
