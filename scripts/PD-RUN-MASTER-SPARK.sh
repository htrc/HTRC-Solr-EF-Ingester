#!/bin/bash

json_filelist=${1:-pd-file-listing-step10000.txt}
shift

input_dir="hdfs://master:9000/user/htrc/pd-ef-json-files"
#input_dir="hdfs://10.10.0.52:9000/user/htrc/pd-ef-json-files"

#output_dir=hdfs://master:9000/user/htrc/pd-solr-json-files
solr_url="http://10.11.0.53:8983/solr/htrc-pd-ef/update"

master_opt="--master spark://10.10.0.52:7077"

. scripts/_RUN.sh

