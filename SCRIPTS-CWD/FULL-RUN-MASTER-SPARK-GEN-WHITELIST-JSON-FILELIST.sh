#!/bin/bash

json_filelist=${1:-full-listing-step100000.txt}
shift

input_dir="hdfs://$SPARK_MASTER_HOST:9000/tmp/dbbridge/full-ef-json-files"
#input_dir="hdfs://master:9000/user/htrc/full-ef-json-files"
#input_dir="hdfs://10.10.0.52:9000/user/htrc/full-ef-json-files"

#output_dir=hdfs://master:9000/user/htrc/full-solr-json-files
solr_url="http://gc0:8983/solr/htrc-full-ef/update"

# 

master_opt="--executor-memory 10g --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError --master spark://$SPARK_MASTER_HOST:7077"

classmain="org.hathitrust.extractedfeatures.ProcessForWhitelistJSONFilelist"

. SCRIPTS-CWD/_RUN.sh

