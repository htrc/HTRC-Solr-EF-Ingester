#!/bin/bash

argc=$#

if [ "$argc" != "1" ] && [ "$argc" != "2" ] ; then
    echo "Usage FULL-DOWNLOAD-EVERY-N.sh <num> [output-dir]" >&2
    exit 1
fi

num=$1

if [ "$argc" = "2" ] ; then
  output_dir="$2"
else
  output_dir="full-ef-json-files"
fi

# Remove any trailing /.
output_dir="${output_dir%/\.}"
output_dir="${output_dir%/}"

if [ ! -d "$output_dir" ] ; then
  echo ""
  echo "****"
  echo "* Creating $output_dir"
  echo "****"
  echo ""
  mkdir "$output_dir"
fi

echo "****"
echo "* Command run: FULL-DOWNLOAD-EVERY-N.sh $*"
echo "****"
echo ""

# Now ensure a trailing /. is present!
output_dir="${output_dir}/."

echo ""
echo "Processing input file: full-listing-step${num}.txt"
echo ""

echo "****"
echo "* Start time: `date`"
echo "****"
echo ""

rsync -W -pav --files-from=full-listing-step${num}.txt \
    data.analytics.hathitrust.org::features/ $output_dir

echo "****"
echo "* End time: `date`"
echo "****"
echo ""