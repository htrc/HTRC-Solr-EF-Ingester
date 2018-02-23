#!/bin/bash

# For standalone compilation:
#  mvn package

# For running on Spark cluster:
#mvn assembly:assembly -DdescriptorId=jar-with-dependencies

mvn package

