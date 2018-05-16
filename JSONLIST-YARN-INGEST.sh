#!/bin/bash

solr_col=TMP-faceted-htrc-fictsample-ef20

solr_config=htrc_configs

# cat file.json | \
#   python -c "import sys, json; print json.load(sys.stdin)['name']"

# http://solr1-s/solr/admin/collections?action=CREATE&name=$solr_col&numShards=4&replicationFactor=1&maxShardsPerNode=-1&collection.configName=$solr_config

# Check to see if a collectcion exists:
#
# http://solr1-s/solr/admin/collections?action=list
#
#{
#    "responseHeader":{
#	"status":0,
#	"QTime":5},
#   "collections":["htrc-test",
#		   "faceted-htrc-full-ef20",
#		   "faceted-htrc-fictsample-ef20",
#		   "htrc-full-ef",
#		   "foo4",
#		   "htrc-full-ef20",
#		   "foo5"]}


nohup ./SCRIPTS-CWD/JSONLIST-RUN-YARN-SPARK.sh \
      pair-tree-annika-1k-fiction-vol-ids.txt TMP-faceted-htrc-fictsample-ef20 &
