#!/bin/bash

scala -classpath ./target/sparknbody-0.0.0.jar GenerateInput \
          /scratch/jmg3/spark-inputs/nbody/points 100 100 \
          /scratch/jmg3/spark-inputs/nbody/info /scratch/jmg3/spark-inputs/nbody/pairs 1000000
