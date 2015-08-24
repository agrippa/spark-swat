#!/bin/bash

scala -classpath ./target/sparknbody-0.0.0.jar GenerateInput ./input/points 10 \
          100 ./input/info ./input/pairs 100000
