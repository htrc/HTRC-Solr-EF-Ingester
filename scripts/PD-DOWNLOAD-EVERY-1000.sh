#!/bin/bash

if [ ! -d pd-ef-json-files ] ; then
  mkdir pd-ef-json-files
fi

rsync -pav --progress --files-from=pd-file-listing-step1000.txt data.analytics.hathitrust.org::pd-features/ pd-ef-json-files/.
