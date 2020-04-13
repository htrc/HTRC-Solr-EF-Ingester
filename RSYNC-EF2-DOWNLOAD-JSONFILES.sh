#!/bin/bash

input_file=${1:-stubby-ids.txt}
output_dir=${2:-json-files-stubby}

if [ ! -d $output_dir ] ; then
    echo "Making directory: $output_dir"
    mkdir $output_dir
fi

echo "Getting EF2 json.bz2 files listed in: $input_file"
echo "Downloading them to: $output_dir"

rsync -av --files-from $input_file queenpalm.ischool.illinois.edu::features-2020.03/ $output_dir
