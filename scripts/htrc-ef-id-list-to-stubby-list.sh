#!/bin/bash

input="$1"

echo "Coverting ids to stubby format:" >&2

while IFS= read -r line
do
    echo "  $line" >&2
    htrc-ef-id-to-stubby-file.sh "$line"
done < "$input"
