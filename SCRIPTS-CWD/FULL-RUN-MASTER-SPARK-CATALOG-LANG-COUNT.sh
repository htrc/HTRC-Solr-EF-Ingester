#!/bin/bash

json_filelist=${1:-full-listing-step100000.txt}
shift

input_dir="hdfs://$SPARK_MASTER_HOST:9000/user/dbbridge/full-ef-json-files"

solr_url="http://gc0:8983/solr/htrc-full-ef/update"


##master_opt="--executor-memory 10g --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError --master spark://$SPARK_MASTER_HOST:7077"

#--conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError 
master_opt="--driver-memory 30g --executor-memory 50g --master spark://$SPARK_MASTER_HOST:7077"

classmain="org.hathitrust.extractedfeatures.ProcessForCatalogLangCount"

. SCRIPTS-CWD/_RUN.sh

