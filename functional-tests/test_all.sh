#!/bin/bash

./test_all_helper.sh &> log
cat log | grep SWAT
