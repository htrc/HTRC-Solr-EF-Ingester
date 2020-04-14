#/bin/bash

output_pairtree_file="ef15-pairtree-file-listing-all.txt"

echo "Getting EF1.5 file-listing (pairtree format)"
rsync -azv data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt "$output_pairtree_file"
echo "Saved as '$output_pairtree_file'"

