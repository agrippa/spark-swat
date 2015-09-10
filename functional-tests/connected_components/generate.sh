#!/bin/bash

# usage: GenerateInput output-links-dir n-output-links-files ndocs mean-nlinks nlinks-range output-docs-file
rm -f input/links/* input/info
scala -classpath ./target/sparkconnectedcomponents-0.0.0.jar GenerateInput \
          ./input/links 10 10000 30000 ./input/info
