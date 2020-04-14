#!/bin/bash

input="$1"
while IFS= read -r line
do
    htrc-ef-id-to-stubby-file.sh "$line"
done < "$input"
