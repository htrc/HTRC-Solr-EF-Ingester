#!/bin/bash

input_file="$1"

if [ ".${input_file##*.}" = ".bz2" ] ; then
    bzcat "$input_file" | python -mjson.tool
else
    cat "$input_file" | python -mjson.tool
fi

    
