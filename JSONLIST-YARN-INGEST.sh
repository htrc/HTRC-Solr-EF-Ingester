#!/bin/bash

. ./scripts/env-check.sh

default_solr_col="$USER-fict1055-htrc-baseline"
default_solr_config="htrc_configs"
default_num_shards=4
default_hdfs_json_list="/user/dbbridge/pair-tree-annika-1k-fiction-vol-ids.txt"

if [ "x$1" = "x-h" ] || [ "x$1" = "x-?" ] ; then
    echo "Usage:" >&2
    echo "  $0 [solr-col] [solr-config] [num-shards] [hdfs-file.txt]" >&2
    echo "Defaults to:" >&2
    echo "  $0 $default_solr_col $default_solr_config $default_num_shards $default_hdfs_json_list" >&2
    exit -1
fi


solr_col=${1:-$default_solr_col}
solr_config=${2:-$default_solr_config}

num_shards=${3:-$default_num_shards};
hdfs_json_list="${4:-$default_hdfs_json_list}"

SOLR_NODES_ARRAY=($SOLR_NODES)
solr_host_with_port=${SOLR_NODES_ARRAY[0]}

solr_endpoint="http://$solr_host_with_port/solr/admin"
solr_cmd="$solr_endpoint/collections?action=list"

echo ""
echo "Checking if collection '$solr_col' exists: "
col_exists=`wget -q "$solr_cmd" -O - \
    | python -c "import sys, json; cols=json.load(sys.stdin)['collections']; print '$solr_col' in cols" `

if [ "x$col_exists" != "x" ] ; then
    # running command produced a result
    if [ "$col_exists" = "True" ] ; then
	echo "  Exists"
    else
	echo "  Does not exist."
	echo ""
	echo "You can create the collection through the Solr admin interface, for example:"
	echo "  wget \"$solr_endpoint/collections?action=CREATE&name=$solr_col&numShards=$num_shards&replicationFactor=1&collection.configName=$solr_config\" -O -"
	echo ""
	echo "Collectcion '$solr_col' could not be found on Solr endpoint '$solr_endpoint'. Exiting" >&2
	exit -1
    fi
else
    echo "Something went wrong running the command:" >&2
    echo " $solr_cmd" >&2
    echo "Exiting" >&2
    exit -1
fi



nohup_cmd="./SCRIPTS-CWD/JSONLIST-RUN-YARN-SPARK.sh $hdfs_json_list $solr_col"
echo ""
echo "Launching nohup cmd:"
echo "  $nohup_cmd"

nohup $nohup_cmd &

#nohup ./SCRIPTS-CWD/JSONLIST-RUN-YARN-SPARK.sh \
#      pair-tree-annika-1k-fiction-vol-ids.txt $solr_col &
