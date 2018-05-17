#!/bin/bash

json_filelist=${1:-full-listing-step100000.txt}
shift

input_dir="hdfs://$SPARK_MASTER_HOST:9000/user/dbbridge/packed-full-ef-part-00000"
#input_dir="hdfs://$SPARK_MASTER_HOST:9000/user/dbbridge/unpacked-ef-10000"

#input_dir="hdfs://$SPARK_MASTER_HOST:9000/user/dbbridge/full-ef-json-files"
#input_dir="hdfs://$SPARK_MASTER_HOST:9000/tmp/dbbridge/full-ef-json-files"
#input_dir="hdfs://master:9000/user/htrc/full-ef-json-files"
#input_dir="hdfs://10.10.0.52:9000/user/htrc/full-ef-json-files"

#output_dir=hdfs://master:9000/user/htrc/full-solr-json-files
#solr_url="http://gc0:8983/solr/htrc-full-ef/update"
solr_url="http://solr1-s:8983/solr/htrc-full-ef/update"

# 

#master_opt="--driver-memory 50g --executor-memory 12g --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError --master spark://$SPARK_MASTER_HOST:7077"
#master_opt="--driver-memory 50g --executor-memory 90g --master spark://$SPARK_MASTER_HOST:7077"
master_opt="--num-executors 33 --executor-cores 4 --driver-memory 20g --executor-memory 8g --master yarn --deploy-mode cluster"

. SCRIPTS-CWD/_RUN.sh

