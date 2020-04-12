#!/bin/bash

default_json_oneliners="page-level-concepts-oneliners.json"

if [ $# = 1 ] ; then
    json_oneliners=$default_json_oneliners
    solr_col=$1
    shift
elif [ $# = 2 ] ; then
    json_oneliners=$1
    shift
    solr_col=$1
    shift
else
    echo "Usage: $0 [json-oneliners.json, default=$json_oneliners_default] solr-col" 1>&2
    exit 1;
fi

echo "Away to incrementally ingest json-onliners '$json_oneliners' into solr collection '$solr_col'"

solr_base_url="http://solr1-s:8983/solr"
#solr_base_url="http://solr1-s/robust-solr"

echo "****"
echo "Using solr_base_url: $solr_base_url"
echo "****"

master_opt="--num-executors 33 --executor-cores 4 --driver-memory 20g --executor-memory 8g --master yarn --deploy-mode cluster"

classmain="org.hathitrust.extractedfeatures.ProcessForConceptIncrementalIngest"

. SCRIPTS-CWD/_RUN_WITH_OPTIONS.sh

