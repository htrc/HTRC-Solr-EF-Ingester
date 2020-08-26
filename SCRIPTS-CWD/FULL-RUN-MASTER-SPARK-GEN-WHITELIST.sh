#!/bin/bash

seq_file=hdfs:///user/capitanu/data/packed-ef-2.0

output_dir=htrc-ef2-whitelist-wordcount

#input_dir="hdfs://$SPARK_MASTER_HOST:9000/tmp/dbbridge/full-ef-json-files"
#input_dir="hdfs://master:9000/user/htrc/full-ef-json-files"
#input_dir="hdfs://10.10.0.52:9000/user/htrc/full-ef-json-files"

#output_dir=hdfs://master:9000/user/htrc/full-solr-json-files
#solr_url="http://gc0:8983/solr/htrc-full-ef/update"

# 

#master_opt="--executor-memory 10g --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError --master spark://$SPARK_MASTER_HOST:7077"
master_opt="--num-executors 33 --executor-cores 4 --driver-memory 20g --executor-memory 8g --master yarn --deploy-mode cluster"

classmain="org.hathitrust.extractedfeatures.ProcessForWhitelist"

. SCRIPTS-CWD/_RUN.sh

