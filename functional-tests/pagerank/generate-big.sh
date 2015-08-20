#!/bin/bash

# usage: GenerateInput output-links-dir n-output-links-files ndocs mean-nlinks nlinks-range output-docs-file
scala -classpath ./target/sparkpagerank-0.0.0.jar GenerateInput /scratch/jmg3/spark-inputs/pagerank/links 100 100000 100 10 /scratch/jmg3/spark-inputs/pagerank/docs
