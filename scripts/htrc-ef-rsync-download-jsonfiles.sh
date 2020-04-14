#!/bin/bash

input_file="${1:-pairtree-ids.txt}"
output_dir="${2:-json-files-pairtree}"

if [ ! -d "$output_dir" ] ; then
    echo "Making directory: '$output_dir'"
    mkdir "$output_dir"
fi

echo "Getting EF1.5 json.bz2 files listed in: '$input_file'"
echo "Downloading them to: '$output_dir'"

rsync -av --progress --files-from "$input_file" data.analytics.hathitrust.org::features/ "$output_dir"

