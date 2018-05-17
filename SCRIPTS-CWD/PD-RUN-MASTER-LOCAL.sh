#!/bin/bash

json_filelist="file://`pwd`/${1:-pd-file-listing-step10000.txt}"
shift

input_dir="file://`pwd`/pd-ef-json-files"
output_dir="file://`pwd`/pd-solr-json-files"

master_opt="--master local[4]"

. SCRIPTS-CWD/_RUN.sh 

