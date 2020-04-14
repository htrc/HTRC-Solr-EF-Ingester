#!/bin/bash

# Looks like 'mvn package' is configured (or is now!) to generate both
# standalone and Spark cluster versions

# For compiling to run run on Spark cluster:
#   mvn assembly:assembly -DdescriptorId=jar-with-dependencies

mvn package

if [ $? = "0" ] ; then
    echo ""
    echo "****"
    echo "* Regenerated target jar files:"
    echo "****"
    date
    ls -l target/htrc-ef-ingest-*.jar
    echo "####"
fi
