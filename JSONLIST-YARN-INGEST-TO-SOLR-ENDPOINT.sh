#!/bin/bash

. ./scripts/env-check.sh

default_solr_base_url="http://solr1-s:9983/solr"
#default_solr_base_url="http://solr1-s/solr8"

default_solr_col="$USER-fict1055-htrc-docvals"
default_solr_config="htrc-configs-docvals"
default_num_shards=4
default_hdfs_json_list="/user/dbbridge/pair-tree-annika-1k-fiction-vol-ids.txt"

if [ "x$1" = "x-h" ] || [ "x$1" = "x-?" ] ; then
    echo "Usage:" >&2
    echo "  $0 [solr-base-url] [solr-col] [solr-config] [num-shards] [hdfs-file.txt]" >&2
    echo "Defaults to:" >&2
    echo "  $0 $defult_solr_base_url $default_solr_col $default_solr_config $default_num_shards $default_hdfs_json_list" >&2
    exit -1
fi

solr_base_url=${1:-$default_solr_base_url}

solr_col=${2:-$default_solr_col}
solr_config=${3:-$default_solr_config}

num_shards=${4:-$default_num_shards};
hdfs_json_list="${5:-$default_hdfs_json_list}"


solr_admin_url="$solr_base_url/admin"
solr_cmd="$solr_admin_url/collections?action=list"

echo "#"
echo "# Checking if collection '$solr_col' exists: "
col_exists=`wget $OPT_WGET_AUTHENTICATE -q "$solr_cmd" -O - \
    | python -c "import sys, json; cols=json.load(sys.stdin)['collections']; print '$solr_col' in cols" `

if [ "x$col_exists" != "x" ] ; then
    # running command produced a result
    if [ "$col_exists" = "True" ] ; then
	echo "#  Exists => Proceeding with ingest"
    else
	echo "#  Does not exist."
	echo ""
#	echo "You can create the collection through the Solr admin interface, for example:"
#	echo "  wget \"$solr_admin_url/collections?action=CREATE&name=$solr_col&numShards=$num_shards&replicationFactor=1&collection.configName=$solr_config\" -O -"
	echo "!!!!"
	echo "! Collectcion '$solr_col' could not be found on Solr endpoint '$solr_admin_url'. Exiting" >&2
	echo "!!!!"
	exit -1
    fi
else
    echo "Something went wrong running the command:" >&2
    echo " $solr_cmd" >&2
    echo "Exiting" >&2
    exit -1
fi



nohup_cmd="./SCRIPTS-CWD/JSONLIST-RUN-YARN-SPARK-TO-SOLR-ENDPOINT.sh $solr_base_url $hdfs_json_list $solr_col"
#nohup_cmd="./SCRIPTS-CWD/JSONLIST-RUN-YARN-SPARK-TO-SOLR-ENDPOINT.sh $solr_base_url $hdfs_json_list $solr_col hdfs:///user/dbbridge/json-files-stubby"

echo ""
echo "Launching nohup cmd:"
echo "  $nohup_cmd"

if [ -f "nohup.out" ] ; then
    echo "****"
    echo "* Do you want to remove nohup.out before it starts [y/N or a=abort]?"
    echo "****"
    read rm_nohup_ans
    if [ "x$rm_nohup_ans" != "x" ] ; then
	if [ "$rm_nohup_ans" = "a" ] ; then
	    exit -1
	fi
	if [ "$rm_nohup_ans" = "y" ] ; then
	    echo "Deleting nohup.out"
	    /bin/rm -f nohup.out
	fi
    fi
       
fi

nohup $nohup_cmd &

