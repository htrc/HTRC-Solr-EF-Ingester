#!/bin/bash

output_stubby_file="ef2-stubby-file-listing.txt"
output_ids_file="ef2-ids.txt"

echo "Getting EF2 file-listing (stubby format)"
rsync -azv queenpalm.ischool.illinois.edu::features-2020.03/listing/file_listing.txt "$output_stubby_file"
echo "Saved as '$output_stubby_file'"

echo "Getting EF2 IDS"
rsync -azv queenpalm.ischool.illinois.edu::features-2020.03/listing/ids.txt "$output_ids_file"
echo "Saved as '$output_ids_file'"
