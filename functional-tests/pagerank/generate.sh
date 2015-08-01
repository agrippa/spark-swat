#!/bin/bash

# usage: GenerateInput output-links-dir n-output-links-files ndocs mean-nlinks nlinks-range output-docs-file
scala -classpath ./target/sparkpagerank-0.0.0.jar GenerateInput ./input/links 10 10000 10 3 ./input/docs
