#!/bin/bash

seq_file="hdfs:///user/capitanu/data/packed-ef"


# --solr-base-url http://solr1-s:8983/solr
#    --properties /homea/dbbridge/extracted-features-solr/solr-ingest/ef-solr.properties hdfs:///user/capitanu/data/packed-ef faceted-htrc-full-ef20


#solr_base_url="http://solr1-s:8983/solr"
#solr_base_url="http://solr3-s:8983/solr"
solr_base_url="http://solr1-s/robust-solr"

#master_opt="--driver-memory 50g --executor-memory 12g --conf spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError --master spark://$SPARK_MASTER_HOST:7077"
#master_opt="--driver-memory 50g --executor-memory 90g --master spark://$SPARK_MASTER_HOST:7077"
master_opt="--num-executors 33 --executor-cores 4 --driver-memory 20g --executor-memory 8g --master yarn --deploy-mode cluster"

. SCRIPTS-CWD/_RUN.sh

