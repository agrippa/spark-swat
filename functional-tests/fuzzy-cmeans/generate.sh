#!/bin/bash

scala -classpath ./target/sparkfuzzycmeans-0.0.0.jar GenerateInput ./input 10 10000 3
