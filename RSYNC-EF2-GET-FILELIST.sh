#!/bin/bash

echo "Getting EF2 file-listing (stubby format)"
rsync -azv queenpalm.ischool.illinois.edu::features-2020.03/listing/file_listing.txt ef2-stubby-file-listing.txt
echo "Saved as ef2-stubby-file-listing.txt"

echo "Getting EF2 IDS"
rsync -azv queenpalm.ischool.illinois.edu::features-2020.03/listing/ids.txt ef2-ids.txt
echo "Saved as ef2-ids.txt"
